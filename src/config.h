#if !defined(_BDCI19_CONFIG_H_INCLUDED_)
#define _BDCI19_CONFIG_H_INCLUDED_

#if !defined(MAKE_FASTEST)
//#define MAKE_FASTEST
#endif

#if defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    0
#define ENABLE_LOGGING_TRACE                0
#define ENABLE_LOGGING_DEBUG                0
#define ENABLE_LOGGING_INFO                 0
#else  // !defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    1
#define ENABLE_LOGGING_TRACE                1
#define ENABLE_LOGGING_DEBUG                1
#define ENABLE_LOGGING_INFO                 1
#endif


#endif  // !defined(_BDCI19_CONFIG_H_INCLUDED_)
