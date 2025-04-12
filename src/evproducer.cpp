#include <iostream>
#include <cerrno>
#include <fstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <cstdint>
#include <unistd.h>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <zmq_addon.hpp>

#include "queue.hpp"
#include "dma-proxy.h"
#include "argparse.hpp"

// TODO
// - define a max size for file event cache

#define EVENT_CACHE_FILE      "/tmp/event.cache"
#define ETH0_TX_BYTES_FILE    "/sys/class/net/eth0/statistics/tx_bytes"
#define MAX_FLUSH_SLEEP_US    2000000
#define MAX_FLUSH_BANDWITH    80    // Mbit/s
#define FLUSH_LATENCY_US      2800

// command line arguments
bool debug = false;
bool local = false;
std::string host;
uint16_t port;
uint16_t mpmtid;
uint16_t size;
std::string mode;
bool runcontrol;

// DMA
const std::string rx_channel_name = "dma_proxy_rx";

struct channel {
   struct channel_buffer *buf_ptr;
   int fd;
   pthread_t tid;
};

struct channel rx_channel;

// ZMQ
zmq::context_t ctx;

// run control
std::string run_state;

// queues
TSQueue<uint16_t> q1, q2, q3;

// cache file
std::string fname;
uint64_t cached_buffers = 0;
std::mutex fmtx;              // event cache mutex
std::mutex smtx;              // ZMQ socket mutex
bool flush_allowed = false;

// ZMQ transfer mutex
zmq::socket_t sock;

// bandwith monitor
uint64_t tx_bandwith;            // bytes/s
uint64_t tx_bandwith_mbps;       // mbit/s
uint16_t tx_flush_bandwith_mbps;

// flush control
std::mutex tmtx;
uint32_t flush_sleep_us = MAX_FLUSH_SLEEP_US;

// run control THREAD
auto control = [](std::string thread_id) {

   zmq::socket_t sock(ctx, zmq::socket_type::sub);
   const std::string url = "tcp://" + host + ":4444";
   sock.connect(url);
   sock.set(zmq::sockopt::subscribe, "control");

   std::cout << "I: control thread subscribed to " << url << std::endl;

   while(true) {

      zmq::message_t topic;
      zmq::message_t payload;

      sock.recv(topic);
      sock.recv(payload);

      if(payload.to_string_view() == "start")
         run_state = "running";
      else if(payload.to_string_view() == "stop")
         run_state = "stopped";

      std::cout << "I: " << payload.to_string_view() << std::endl;

      std::this_thread::sleep_for(std::chrono::milliseconds(500));
   }
};

// DMA transfer THREAD
auto dma_transfer = [](std::string thread_id) {

   int32_t buffer_id;      // int32_t due to dma-proxy ioctl prototype: _IOW('a', 'b', int32_t *)
   proxy_status status;
   bool next_xfer = true;

   // initialize DMA buffers area
   memset(rx_channel.buf_ptr, 0, sizeof(struct channel_buffer));
   
   buffer_id = q1.pop();

   while(true) {

      if(run_state == "running") {

         rx_channel.buf_ptr[buffer_id].length = size;                  
         if(next_xfer) 
            ioctl(rx_channel.fd, START_XFER, &buffer_id);
         ioctl(rx_channel.fd, FINISH_XFER, &buffer_id);
         status = rx_channel.buf_ptr[buffer_id].status;

         if(debug)
            std::cout << thread_id << ": buffer " << buffer_id << " transfer completed - status: " << 
               status << " transferred_length: " << rx_channel.buf_ptr[buffer_id].transferred_length << std::endl;

         if (status == PROXY_NO_ERROR) {

            next_xfer = true;
   
            if(mode == "local")        // in local mode reinsert completed buffer id
               q1.push(buffer_id);
            else                       // in remote mode send completed buffer id to zmq_transfer thread
               q2.push(buffer_id);

         if(debug) {
            uint8_t *buf = reinterpret_cast<uint8_t *>(rx_channel.buf_ptr[buffer_id].buffer);
            for(int i=0; i<16; i++)
               printf("0x%X - ", buf[i]);
            printf("\n");
         }

            buffer_id = q1.pop();

         } else next_xfer = false;     // retry current transfer

      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));
   }
};

// ZMQ transfer THREAD
auto zmq_transfer = [](std::string thread_id) {

   uint16_t buffer_id;

   while(true) {

      if(run_state == "running") {

         buffer_id = q2.pop();

         zmq::const_buffer buf = zmq::buffer(rx_channel.buf_ptr[buffer_id].buffer, rx_channel.buf_ptr[buffer_id].transferred_length);

         smtx.lock();
            auto rc = sock.send(buf, zmq::send_flags::dontwait);
         smtx.unlock();

         /*
          * rc holding the return of a send(), a zmq::send_result_t 
          * that is a std::optional which may hold a size_t giving 
          * the number of bytes sent or nothing if EAGAIN error occurred
         */

         if(rc.has_value()) {
            
            if(rc.value() != rx_channel.buf_ptr[buffer_id].transferred_length) {          // send failed (partial send)

               std::cout << thread_id << ": partial buffer sent " << rc.value() << std::endl;

            } else {       // send success
               
               q1.push(buffer_id);
            }

         } else {

            if(errno == EAGAIN)                                   // send failed (bandwith full)....
               q3.push(buffer_id);                                // wake-up cache thread...
         }

      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));
   }
};

// DISK cache THREAD
auto disk_cache = [](std::string thread_id) {

   uint16_t buffer_id;
   char *bufchar;

   std::ofstream evcache;
   evcache = std::ofstream(fname, std::ios::out | std::ios::binary);

   while (true) {

      if(run_state == "running") {

         buffer_id = q3.pop();

         bufchar = reinterpret_cast<char *>(rx_channel.buf_ptr[buffer_id].buffer);

         fmtx.lock();
            evcache.seekp(0, evcache.end);
            evcache.write(bufchar, rx_channel.buf_ptr[buffer_id].length);
            evcache.flush();     // commit changes
            cached_buffers++;
         fmtx.unlock();

         q1.push(buffer_id);
          
      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));
   }
};

// DISK flush THREAD
auto disk_flush = [](std::string thread_id) {

   char *cbuf; 
   uint64_t flush_tx_bytes, prev_flush_tx_bytes;
   std::chrono::steady_clock::time_point start;

   std::ifstream evcache;
   prev_flush_tx_bytes = flush_tx_bytes = 0;

   cbuf = new char[BUFFER_SIZE];

   start = std::chrono::steady_clock::now();

   while (true) {

      // update stats
      std::chrono::steady_clock::duration time_elapsed = std::chrono::steady_clock::now() - start;
      long time_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_elapsed).count();

      if(time_elapsed_ms >= 1000) {

         tx_flush_bandwith_mbps = ((flush_tx_bytes - prev_flush_tx_bytes) / float(time_elapsed_ms / 1000.0)) * 8 / 1024 / 1024;

         // due to TCP buffering application level bandwith can be greater than maximum...
         tx_flush_bandwith_mbps = std::min((uint16_t)100, tx_flush_bandwith_mbps);

         start = std::chrono::steady_clock::now();
         prev_flush_tx_bytes = flush_tx_bytes;
      }

      if(run_state == "running") {

retry:
         flush_allowed = (q3.size() == 0);

         if(cached_buffers > 0) {

            if(!evcache.is_open())
               evcache.open(fname, std::ios::binary);

            std::this_thread::sleep_for(std::chrono::microseconds(flush_sleep_us));

            if(flush_allowed) {

               int8_t avail_bandwith = MAX_FLUSH_BANDWITH - tx_bandwith_mbps + tx_flush_bandwith_mbps;

               if(avail_bandwith <= 0) {

                  tmtx.lock();
                     flush_sleep_us = MAX_FLUSH_SLEEP_US;
                  tmtx.unlock();
                  goto retry;

               } else {

                  uint32_t avail_bandwith_bs = (avail_bandwith * 1024 * 1024) / 8;
                  float value = ((1.0 / (avail_bandwith_bs/BUFFER_SIZE)) * 1000 * 1000) - FLUSH_LATENCY_US;
                  tmtx.lock();
                     flush_sleep_us = value;
                  tmtx.unlock();
               }

               fmtx.lock();
                  evcache.sync();
                  evcache.seekg(0, evcache.end);
                  evcache.seekg(-BUFFER_SIZE, evcache.cur);
                  evcache.read(cbuf, BUFFER_SIZE);

                  zmq::message_t msg(cbuf, BUFFER_SIZE);

                  smtx.lock();
                     auto rc = sock.send(std::move(msg), zmq::send_flags::dontwait);
                  smtx.unlock();

                  if(rc.has_value()) {

                     if(rc.value() != BUFFER_SIZE) {       // send failed

                        std::cout << thread_id << ": partial buffer sent " << rc.value() << std::endl;

                     } else {    // send success

                        // update stats
                        flush_tx_bytes += BUFFER_SIZE;

                        // resize event cache file...
                        std::uintmax_t fsize = std::filesystem::file_size(fname);
                        std::uintmax_t newfsize;

                        if(fsize < BUFFER_SIZE) {
                           newfsize = 0;
                           cached_buffers -= fsize / BUFFER_SIZE;
                        } else {
                           newfsize = fsize - BUFFER_SIZE;
                           cached_buffers--;
                        } 

                        std::filesystem::resize_file(fname, newfsize);
                     }

                  } 
               fmtx.unlock();

            } // end if flush_allowed
            
         } else { // end if (cached_buffers > 0)
               
            flush_sleep_us = MAX_FLUSH_SLEEP_US;
            std::this_thread::sleep_for(std::chrono::microseconds(flush_sleep_us));
         }

      } else std::this_thread::sleep_for(std::chrono::milliseconds(50));

   }  // end while(true)
};

int main(int argc, const char **argv) {

   argparse::ArgumentParser program("evproducer");

   // START parsing command line options

   program.add_argument("--local")
    .help("local mode")
    .default_value(false)
    .implicit_value(true);

   program.add_argument("--debug")
    .help("enable debug")
    .default_value(false)
    .implicit_value(true);

   program.add_argument("--host")
    .help("receiver hostname");

   program.add_argument("--port")
    .help("receiver port")
    .default_value(uint16_t(5555))
    .scan<'u', uint16_t>();

   program.add_argument("--id")
    .help("MPMT id")
    .default_value(uint16_t(1))
    .scan<'u', uint16_t>();

   program.add_argument("--size")
    .help("DMA block size")
    .default_value(uint16_t(32768))
    .scan<'u', uint16_t>();

   program.add_argument("--disable-rc")
    .help("disable ZMQ run control")
    .default_value(false)
    .implicit_value(true);

   try {
      program.parse_args(argc, argv);
   } catch (const std::runtime_error& err) {
      std::cerr << err.what() << std::endl;
      std::cerr << program;
      return EXIT_FAILURE;
   }

   if(program.get<bool>("--local")) {
      mode = "local";
   } else if(program.present("--host")) {
      host = program.get<std::string>("--host");
      port = program.get<uint16_t>("--port");
      mpmtid = program.get<uint16_t>("--id");
      mode = "remote";
   } else {
      std::cerr << "E: specify local or host/id options" << std::endl;
      return EXIT_FAILURE;
   }

   size = program.get<uint16_t>("--size");
   debug = program.get<bool>("--debug");
   runcontrol = !(program.get<bool>("--disable-rc"));

   // END parsing command line options

   // open eth0 tx statistics
   std::ifstream eth0_stats_tx = std::ifstream(ETH0_TX_BYTES_FILE);

   // DMA

   std::string channel_name = "/dev/" + rx_channel_name;
   rx_channel.fd = open(channel_name.c_str(), O_RDWR);
   if (rx_channel.fd < 1) {
      std::cout << "E: unable to open DMA proxy device file: " << channel_name << std::endl;
      exit(EXIT_FAILURE);
   }

   rx_channel.buf_ptr = (struct channel_buffer *)mmap(NULL, sizeof(struct channel_buffer) * RX_BUFFER_COUNT,
      PROT_READ | PROT_WRITE, MAP_SHARED, rx_channel.fd, 0);
   if (rx_channel.buf_ptr == MAP_FAILED) {
      std::cout << "E: failed to mmap rx channel" << std::endl;
      exit(EXIT_FAILURE);
   }

   std::thread th0, th1, th2, th3, th4;
   if(mode == "remote") {

      sock = zmq::socket_t(ctx, zmq::socket_type::dealer);

      const std::string url = "tcp://" + host + ":" + std::to_string(port);
      sock.set(zmq::sockopt::routing_id, std::to_string(mpmtid));
      /* queue messages only to completed connections */
      sock.set(zmq::sockopt::immediate, 1);
      /* set linger period for socket shutdown - 0 no linger period */
      sock.set(zmq::sockopt::linger, 0);
      sock.connect(url);

      if(runcontrol)
         th0 = std::thread(control, "control");
      else run_state = "running";

      th2 = std::thread(zmq_transfer, "zmq_xfer");

      fname = std::string(EVENT_CACHE_FILE) + "." + std::to_string(std::time(0));

      th3 = std::thread(disk_cache, "disk_cache");
      th4 = std::thread(disk_flush, "disk_flush");
   } else run_state = "running";

   // before main loop preload DMA transfer queue
   for(uint16_t i=0; i<RX_BUFFER_COUNT; i++)
      q1.push(i);

   th1 = std::thread(dma_transfer, "dma_xfer");

   uint64_t prev_tx_bytes, tx_bytes;
   std::string line; 

   // main loop

   // network bandwith monitor
   eth0_stats_tx.sync();
   eth0_stats_tx.seekg(0);
   line.clear();
   std::getline(eth0_stats_tx, line);
   prev_tx_bytes = std::stoll(line);

   while(true) {

      if(run_state == "running") {

         // network bandwith monitor
         eth0_stats_tx.clear();
         eth0_stats_tx.seekg(0);
         line.clear();
         std::getline(eth0_stats_tx, line);
         tx_bytes = std::stoll(line);

         if(tx_bytes >= prev_tx_bytes)
            tx_bandwith = tx_bytes - prev_tx_bytes;
         else
            tx_bandwith = std::numeric_limits<uint32_t>::max() - prev_tx_bytes + tx_bytes;

         tx_bandwith_mbps = (tx_bandwith * 8) / 1024 / 1024;
         prev_tx_bytes = tx_bytes;

         // some stats here...
         std::cout << "Q1/Q2/Q3 size: " << q1.size() << "/" << q2.size() << "/" << q3.size() << " , buffers cached/tx.allowed: " << cached_buffers << "/" << flush_allowed << " , eth0.tx (Mbit/s): " << tx_bandwith_mbps << " , flush.tx (Mbit/s): " << tx_flush_bandwith_mbps << " , flush.sleep (us): " << flush_sleep_us << std::endl;

      }

      sleep(1);

   } // end while

   if(mode == "remote") {
      if(runcontrol)
         th0.join();
      th2.join();
      th3.join();
      th4.join();
   }

   th1.join();
   eth0_stats_tx.close();

   // DMA close
   munmap(rx_channel.buf_ptr, sizeof(struct channel_buffer));
   close(rx_channel.fd);

   std::cout << "Bye!" << std::endl;

   return EXIT_SUCCESS;
}
