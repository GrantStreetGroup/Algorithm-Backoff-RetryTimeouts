use strict;
use warnings;

use Test::More;

BEGIN { use_ok('Algorithm::RetryTimeouts') };

diag(qq(Algorithm::RetryTimeouts Perl $], $^X));

done_testing;
