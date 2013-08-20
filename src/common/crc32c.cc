#include "include/crc32c.h"

#include "common/sctp_crc32.h"

/*
 * choose best implementation based on the CPU architecture.
 */
ceph_crc32c_func_t ceph_choose_crc32(void)
{
	/* default */
	return ceph_crc32c_sctp;
}

/*
 * static global
 *
 * This is a bit of a no-no for shared libraries, but we don't care.
 * It is effectively constant for the executing process as the value
 * depends on the CPU architecture.
 *
 * We initialize it during program init using the magic of C++.
 */
ceph_crc32c_func_t ceph_crc32c_func = ceph_choose_crc32();

