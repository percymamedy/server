/* Copyright (c) 2008 MySQL AB, 2009 Sun Microsystems, Inc.
   Use is subject to license terms.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

/*
  rdtsc3 -- multi-platform timer code
  pgulutzan@mysql.com, 2005-08-29
  modified 2008-11-02
*/

#ifndef MY_RDTSC_H
#define MY_RDTSC_H

# ifdef _WIN32
#  include <intrin.h>
# elif defined __i386__ || defined __x86_64__
#  include <x86intrin.h>
# elif defined(__INTEL_COMPILER) && defined(__ia64__) && defined(HAVE_IA64INTRIN_H)
#  include <ia64intrin.h>
# elif defined(HAVE_SYS_TIMES_H) && defined(HAVE_GETHRTIME)
#  include <sys/times.h>
# endif

/**
  Characteristics of a timer.
*/
struct my_timer_unit_info
{
  /** Routine used for the timer. */
  ulonglong routine;
  /** Overhead of the timer. */
  ulonglong overhead;
  /** Frequency of the  timer. */
  ulonglong frequency;
  /** Resolution of the timer. */
  ulonglong resolution;
};

/**
  Characteristics of all the supported timers.
  @sa my_timer_init().
*/
struct my_timer_info
{
  /** Characteristics of the cycle timer. */
  struct my_timer_unit_info cycles;
  /** Characteristics of the nanosecond timer. */
  struct my_timer_unit_info nanoseconds;
  /** Characteristics of the microsecond timer. */
  struct my_timer_unit_info microseconds;
  /** Characteristics of the millisecond timer. */
  struct my_timer_unit_info milliseconds;
  /** Characteristics of the tick timer. */
  struct my_timer_unit_info ticks;
};

typedef struct my_timer_info MY_TIMER_INFO;

C_MODE_START

/*
  For cycles, we depend on RDTSC for x86 platforms,
  or on time buffer (which is not really a cycle count
  but a separate counter with less than nanosecond
  resolution) for most PowerPC platforms, or on
  gethrtime which is okay for hpux and solaris,
  or on read_real_time for aix platforms. There is
  nothing for Alpha platforms, they would be tricky.

  On the platforms that do not have a CYCLE timer,
  "wait" events are initialized to use NANOSECOND instead of CYCLE
  during performance_schema initialization (at the server startup).

  Linux performance monitor (see "man perf_event_open") can
  provide cycle counter on the platforms that do not have
  other kinds of cycle counters. But we don't use it so far.

  ARM notes
  ---------
  During tests on ARMv7 Debian, perf_even_open() based cycle counter provided
  too low frequency with too high overhead:
  MariaDB [performance_schema]> SELECT * FROM performance_timers;
  +-------------+-----------------+------------------+----------------+
  | TIMER_NAME  | TIMER_FREQUENCY | TIMER_RESOLUTION | TIMER_OVERHEAD |
  +-------------+-----------------+------------------+----------------+
  | CYCLE       | 689368159       | 1                | 970            |
  | NANOSECOND  | 1000000000      | 1                | 308            |
  | MICROSECOND | 1000000         | 1                | 417            |
  | MILLISECOND | 1000            | 1000             | 407            |
  | TICK        | 127             | 1                | 612            |
  +-------------+-----------------+------------------+----------------+
  Therefore, it was decided not to use perf_even_open() on ARM
  (i.e. go without CYCLE and have "wait" events use NANOSECOND by default).
*/

/**
  A cycle timer.
  @return the current timer value, in cycles.
*/
static inline ulonglong my_timer_cycles(void)
{
# if defined _WIN32 || defined __i386__ || defined __x86_64__
  return __rdtsc();
# elif defined(__INTEL_COMPILER) && defined(__ia64__) && defined(HAVE_IA64INTRIN_H)
  return (ulonglong) __getReg(_IA64_REG_AR_ITC); /* (3116) */
#elif defined(__GNUC__) && defined(__ia64__)
  {
    ulonglong result;
    __asm __volatile__ ("mov %0=ar.itc" : "=r" (result));
    return result;
  }
#elif defined(__GNUC__) && (defined(__powerpc__) || defined(__POWERPC__) || (defined(_POWER) && defined(_AIX52))) && (defined(__64BIT__) || defined(_ARCH_PPC64))
  {
    ulonglong result;
    __asm __volatile__ ("mftb %0" : "=r" (result));
    return result;
  }
#elif defined(__GNUC__) && (defined(__powerpc__) || defined(__POWERPC__) || (defined(_POWER) && defined(_AIX52))) && (!defined(__64BIT__) && !defined(_ARCH_PPC64))
  {
    /*
      mftbu means "move from time-buffer-upper to result".
      The loop is saying: x1=upper, x2=lower, x3=upper,
      if x1!=x3 there was an overflow so repeat.
    */
    unsigned int x1, x2, x3;
    ulonglong result;
    for (;;)
    {
       __asm __volatile__ ( "mftbu %0" : "=r"(x1) );
       __asm __volatile__ ( "mftb %0" : "=r"(x2) );
       __asm __volatile__ ( "mftbu %0" : "=r"(x3) );
       if (x1 == x3) break;
    }
    result = x1;
    return ( result << 32 ) | x2;
  }
#elif defined(__GNUC__) && defined(__sparcv9) && defined(_LP64)
  {
    ulonglong result;
    __asm __volatile__ ("rd %%tick,%0" : "=r" (result));
    return result;
  }
#elif defined(__GNUC__) && defined(__sparc__) && !defined(_LP64)
  {
      union {
              ulonglong wholeresult;
              struct {
                      ulong high;
                      ulong low;
              }       splitresult;
      } result;
    __asm __volatile__ ("rd %%tick,%1; srlx %1,32,%0" : "=r" (result.splitresult.high), "=r" (result.splitresult.low));
    return result.wholeresult;
  }
#elif defined(__GNUC__) && defined(__s390__)
  /* covers both s390 and s390x */
  {
    ulonglong result;
    __asm__ __volatile__ ("stck %0" : "=Q" (result) : : "cc");
    return result;
  }
#elif defined(HAVE_SYS_TIMES_H) && defined(HAVE_GETHRTIME)
  /* gethrtime may appear as either cycle or nanosecond counter */
  return (ulonglong) gethrtime();
#else
  return 0;
#endif
}

/**
  A nanosecond timer.
  @return the current timer value, in nanoseconds.
*/
ulonglong my_timer_nanoseconds(void);

/**
  A microseconds timer.
  @return the current timer value, in microseconds.
*/
ulonglong my_timer_microseconds(void);

/**
  A millisecond timer.
  @return the current timer value, in milliseconds.
*/
ulonglong my_timer_milliseconds(void);

/**
  A ticks timer.
  @return the current timer value, in ticks.
*/
ulonglong my_timer_ticks(void);

/**
  Timer initialization function.
  @param [out] mti the timer characteristics.
*/
void my_timer_init(MY_TIMER_INFO *mti);

C_MODE_END

#define MY_TIMER_ROUTINE_RDTSC                    5
#define MY_TIMER_ROUTINE_ASM_IA64                 6
#define MY_TIMER_ROUTINE_ASM_PPC                  7
#define MY_TIMER_ROUTINE_GETHRTIME                9
#define MY_TIMER_ROUTINE_READ_REAL_TIME          10
#define MY_TIMER_ROUTINE_CLOCK_GETTIME           11
#define MY_TIMER_ROUTINE_NXGETTIME               12
#define MY_TIMER_ROUTINE_GETTIMEOFDAY            13
#define MY_TIMER_ROUTINE_QUERYPERFORMANCECOUNTER 14
#define MY_TIMER_ROUTINE_GETTICKCOUNT            15
#define MY_TIMER_ROUTINE_TIME                    16
#define MY_TIMER_ROUTINE_TIMES                   17
#define MY_TIMER_ROUTINE_FTIME                   18
#define MY_TIMER_ROUTINE_ASM_PPC64               19
#define MY_TIMER_ROUTINE_ASM_GCC_SPARC64         23
#define MY_TIMER_ROUTINE_ASM_GCC_SPARC32         24
#define MY_TIMER_ROUTINE_MACH_ABSOLUTE_TIME      25
#define MY_TIMER_ROUTINE_GETSYSTEMTIMEASFILETIME 26
#define MY_TIMER_ROUTINE_ASM_S390                28

#endif

