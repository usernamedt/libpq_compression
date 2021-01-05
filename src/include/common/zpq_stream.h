/*
 * zpq_stream.h
 *     Streaiming compression for libpq
 */

#ifndef ZPQ_STREAM_H
#define ZPQ_STREAM_H

#include <stdlib.h>

#define ZPQ_OK (0)
#define ZPQ_IO_ERROR (-1)
#define ZPQ_DECOMPRESS_ERROR (-2)
#define ZPQ_COMPRESS_ERROR (-3)
#define ZPQ_STREAM_END (-4)
#define ZPQ_DATA_PENDING (-5)

#define ZPQ_DEFAULT_COMPRESSION_LEVEL (1)

struct ZpqStream;
typedef struct ZpqStream ZpqStream;

typedef ssize_t(*zpq_tx_func)(void* arg, void const* data, size_t size);
typedef ssize_t(*zpq_rx_func)(void* arg, void* data, size_t size);

/*
 * Create compression stream with using rx/tx function for fetching/sending compressed data.
 * c_alg_impl: index of chosen compression algorithm
 * c_level: compression c_level
 * d_alg_impl: index of chosen decompression algorithm
 * tx_func: function for writing compressed data in underlying stream
 * rx_func: function for receiving compressed data from underlying stream
 * arg: context passed to the function
 * rx_data: received data (compressed data already fetched from input stream)
 * rx_data_size: size of data fetched from input stream
 */
ZpqStream* zpq_create(int c_alg_impl, int c_level, int d_alg_impl, zpq_tx_func tx_func, zpq_rx_func rx_func, void* arg, char* rx_data, size_t rx_data_size);

/*
 * Read up to "size" raw (decompressed) bytes.
 * Returns number of decompressed bytes or error code.
 * Error code is either ZPQ_DECOMPRESS_ERROR or error code returned by the rx function.
 */
ssize_t zpq_read(ZpqStream* zs, void* buf, size_t size);

/*
 * Write up to "size" raw (decompressed) bytes.
 * Returns number of written raw bytes or error code.
 * Error code is either ZPQ_COMPRESS_ERROR or error code returned by the tx function.
 * In case of an error, amount of written raw bytes is stored in *processed.
 */
ssize_t zpq_write(ZpqStream* zs, void const* buf, size_t size, size_t* processed);

/*
 * Get decompressor error message.
 */
char const* zpq_decompress_error(ZpqStream* zs);

/*
 * Get compressor error message.
 */
char const* zpq_compress_error(ZpqStream* zs);

/*
 * Returns an estimated amount of data in internal rx decompression buffer.
 */
size_t zpq_buffered_rx(ZpqStream* zs);

/*
 * Returns an estimated amount of data in internal tx compression buffer.
 */
size_t zpq_buffered_tx(ZpqStream* zs);

/*
 * Free stream created by zpq_create function.
 */
void zpq_free(ZpqStream* zs);

/*
 * Get the name of chosen compression algorithm.
 */
char const* zpq_compress_algorithm_name(ZpqStream* zs);

/*
 * Get the name of chosen decompression algorithm.
 */
char const* zpq_decompress_algorithm_name(ZpqStream* zs);

/*
  Returns zero terminated array with compression algorithms names
*/
char** zpq_get_supported_algorithms(void);

#endif
