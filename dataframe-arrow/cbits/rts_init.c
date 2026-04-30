/* Auto-initialise / finalise the GHC RTS when the shared library is
   loaded / unloaded.  Required because cabal's native-shared libraries
   do not run hs_init automatically on macOS. Haven't tested on other platforms.
   TODO: mchavinda - Implement platform-specific initialization logic. */

#include <stddef.h>
#include "HsFFI.h"

__attribute__((constructor))
static void df_lib_init(void)
{
    hs_init(0, NULL);
}

__attribute__((destructor))
static void df_lib_fini(void)
{
    hs_exit();
}
