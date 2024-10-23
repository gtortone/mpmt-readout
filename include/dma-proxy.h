
#define BUFFER_SIZE 		   (32 * 1024)	 		/* must match driver exactly */
#define RX_BUFFER_COUNT 	9500              /* app only, must be <= to the number in the driver */

#define FINISH_XFER 		_IOW('a','a',int32_t*)
#define START_XFER      _IOW('a','b',int32_t*)
#define XFER 			   _IOR('a','c',int32_t*)

enum proxy_status { PROXY_NO_ERROR = 0, PROXY_BUSY = 1, PROXY_TIMEOUT = 2, PROXY_ERROR = 3 };

struct channel_buffer {
   unsigned int buffer[BUFFER_SIZE / sizeof(unsigned int)];
   proxy_status status;
   unsigned int length;
   unsigned int transferred_length;
} __attribute__ ((aligned (1024)));		/* 64 byte alignment required for DMA, but 1024 handy for viewing memory */
