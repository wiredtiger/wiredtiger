package Stow::Util;

=head1 NAME

Stow::Util - general utilities

=head1 SYNOPSIS

    use Stow::Util qw(debug set_debug_level error ...);

=head1 DESCRIPTION

Supporting utility routines for L<Stow>.

=cut

use strict;
use warnings;

use POSIX qw(getcwd);

use base qw(Exporter);
our @EXPORT_OK = qw(
    error debug set_debug_level set_test_mode
    join_paths parent canon_path restore_cwd
);

our $ProgramName = 'stow';
our $VERSION = '2.2.2';

#############################################################################
#
# General Utilities: nothing stow specific here.
#
#############################################################################

=head1 IMPORTABLE SUBROUTINES

=head2 error($format, @args)

Outputs an error message in a consistent form and then dies.

=cut

sub error {
    my ($format, @args) = @_;
    die "$ProgramName: ERROR: " . sprintf($format, @args) . "\n";
}

=head2 set_debug_level($level)

Sets verbosity level for C<debug()>.

=cut

our $debug_level = 0;

sub set_debug_level {
    my ($level) = @_;
    $debug_level = $level;
}

=head2 set_test_mode($on_or_off)

Sets testmode on or off.

=cut

our $test_mode = 0;

sub set_test_mode {
    my ($on_or_off) = @_;
    if ($on_or_off) {
        $test_mode = 1;
    }
    else {
        $test_mode = 0;
    }
}

=head2 debug($level, $msg)

Logs to STDERR based on C<$debug_level> setting.  C<$level> is the
minimum verbosity level required to output C<$msg>.  All output is to
STDERR to preserve backward compatibility, except for in test mode,
when STDOUT is used instead.  In test mode, the verbosity can be
overridden via the C<TEST_VERBOSE> environment variable.

Verbosity rules:

=over 4

=item    0: errors only

=item >= 1: print operations: LINK/UNLINK/MKDIR/RMDIR/MV

=item >= 2: print operation exceptions

e.g. "_this_ already points to _that_", skipping, deferring,
overriding, fixing invalid links

=item >= 3: print trace detail: trace: stow/unstow/package/contents/node

=item >= 4: debug helper routines

=item >= 5: debug ignore lists

=back

=cut

sub debug {
    my ($level, $msg) = @_;
    if ($debug_level >= $level) {
        if ($test_mode) {
            print "# $msg\n";
        }
        else {
            warn "$msg\n";
        }
    }
}

#===== METHOD ===============================================================
# Name      : join_paths()
# Purpose   : concatenates given paths
# Parameters: path1, path2, ... => paths
# Returns   : concatenation of given paths
# Throws    : n/a
# Comments  : factors out redundant path elements:
#           : '//' => '/' and 'a/b/../c' => 'a/c'
#============================================================================
sub join_paths {
    my @paths = @_;

    # weed out empty components and concatenate
    my $result = join '/', grep {! /\A\z/} @paths;

    # factor out back references and remove redundant /'s)
    my @result = ();
    PART:
    for my $part (split m{/+}, $result) {
        next PART if $part eq '.';
        if (@result && $part eq '..' && $result[-1] ne '..') {
            pop @result;
        }
        else {
            push @result, $part;
        }
    }

    return join '/', @result;
}

#===== METHOD ===============================================================
# Name      : parent
# Purpose   : find the parent of the given path
# Parameters: @path => components of the path
# Returns   : returns a path string
# Throws    : n/a
# Comments  : allows you to send multiple chunks of the path
#           : (this feature is currently not used)
#============================================================================
sub parent {
    my @path = @_;
    my $path = join '/', @_;
    my @elts = split m{/+}, $path;
    pop @elts;
    return join '/', @elts; 
}

#===== METHOD ===============================================================
# Name      : canon_path
# Purpose   : find absolute canonical path of given path
# Parameters: $path
# Returns   : absolute canonical path
# Throws    : n/a
# Comments  : is this significantly different from File::Spec->rel2abs?
#============================================================================
sub canon_path {
    my ($path) = @_;

    my $cwd = getcwd();
    chdir($path) or error("canon_path: cannot chdir to $path from $cwd");
    my $canon_path = getcwd();
    restore_cwd($cwd);

    return $canon_path;
}

sub restore_cwd {
    my ($prev) = @_;
    chdir($prev) or error("Your current directory $prev seems to have vanished");
}

=head1 BUGS

=head1 SEE ALSO

=cut

1;

# Local variables:
# mode: perl
# cperl-indent-level: 4
# end:
# vim: ft=perl
