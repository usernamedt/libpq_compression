/*
 * zpq_stream.h
 *     Streaming compression for libpq
 */

#include "z_stream.h"

#ifndef ZPQ_STREAM_H
#define ZPQ_STREAM_H

#include <stdlib.h>


#define ZPQ_DEFAULT_COMPRESSION_LEVEL (1)

struct ZpqStream;
typedef struct ZpqStream ZpqStream;

typedef ssize_t (*zpq_tx_func) (void *arg, void const *data, size_t size);
typedef ssize_t (*zpq_rx_func) (void *arg, void *data, size_t size);


#endif

/*
 * Create compression stream with rx/tx function for reading/sending compressed data.
 * c_alg_impl: index of chosen compression algorithm
 * c_level: compression c_level
 * d_alg_impl: index of chosen decompression algorithm
 * tx_func: function for writing compressed data in underlying stream
 * rx_func: function for reading compressed data from underlying stream
 * arg: context passed to the function
 * rx_data: received data (compressed data already fetched from input stream)
 * rx_data_size: size of data fetched from input stream
 */
extern ZpqStream * zpq_create(int c_alg_impl, int c_level, int d_alg_impl, zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg, char *rx_data, size_t rx_data_size);

/*
 * Write up to "src_size" raw (decompressed) bytes.
 * Returns number of written raw bytes or error code.
 * Error code is either ZPQ_COMPRESS_ERROR or error code returned by the tx function.
 * In the last case number of bytes written is stored in *src_processed.
 */
extern ssize_t zpq_write(ZpqStream * zs, void const *src, size_t src_size, size_t *src_processed);

/*
 * Read up to "size" raw (decompressed) bytes.
 * Returns number of decompressed bytes or error code.
 * Error code is either ZPQ_DECOMPRESS_ERROR or error code returned by the rx function.
 */
extern ssize_t zpq_read(ZpqStream * zs, void *buf, size_t size);

/*
 * Return true if non-flushed data left in internal rx decompression buffer.
 */
extern bool zpq_buffered_rx(ZpqStream * zs);

/*
 * Return true if non-flushed data left in internal tx compression buffer.
 */
extern bool zpq_buffered_tx(ZpqStream * zs);

/*
 * Free stream created by zs_create function.
 */
extern void zpq_free(ZpqStream * zs);

/*
 * Get decompressor error message.
 */
extern char const *zpq_decompress_error(ZpqStream * zs);

/*
 * Get compressor error message.
 */
extern char const *zpq_compress_error(ZpqStream * zs);

/*
 * Get the name of chosen compression algorithm.
 */
extern char const *zpq_compress_algorithm_name(ZpqStream * zs);
