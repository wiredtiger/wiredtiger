#!/usr/bin/perl

package Stow;

=head1 NAME

Stow - manage the installation of multiple software packages

=head1 SYNOPSIS

    my $stow = new Stow(%$options);

    $stow->plan_unstow(@pkgs_to_unstow);
    $stow->plan_stow  (@pkgs_to_stow);

    my %conflicts = $stow->get_conflicts;
    $stow->process_tasks() unless %conflicts;

=head1 DESCRIPTION

This is the backend Perl module for GNU Stow, a program for managing
the installation of software packages, keeping them separate
(C</usr/local/stow/emacs> vs. C</usr/local/stow/perl>, for example)
while making them appear to be installed in the same place
(C</usr/local>).

Stow doesn't store an extra state between runs, so there's no danger
of mangling directories when file hierarchies don't match the
database. Also, stow will never delete any files, directories, or
links that appear in a stow directory, so it is always possible to
rebuild the target tree.

=cut

use strict;
use warnings;

use Carp qw(carp cluck croak confess longmess);
use File::Copy qw(move);
use File::Spec;
use POSIX qw(getcwd);

use Stow::Util qw(set_debug_level debug error set_test_mode
                  join_paths restore_cwd canon_path parent);

our $ProgramName = 'stow';
our $VERSION = '2.2.2';

our $LOCAL_IGNORE_FILE  = '.stow-local-ignore';
our $GLOBAL_IGNORE_FILE = '.stow-global-ignore';

our @default_global_ignore_regexps =
    __PACKAGE__->get_default_global_ignore_regexps();

# These are the default options for each Stow instance.
our %DEFAULT_OPTIONS = (
    conflicts    => 0,
    simulate     => 0,
    verbose      => 0,
    paranoid     => 0,
    compat       => 0,
    test_mode    => 0,
    adopt        => 0,
    'no-folding' => 0,
    ignore       => [],
    override     => [],
    defer        => [],
);

=head1 CONSTRUCTORS

=head2 new(%options)

=head3 Required options

=over 4

=item * dir - the stow directory

=item * target - the target directory

=back

=head3 Non-mandatory options

See the documentation for the F<stow> CLI front-end for information on these.

=over 4

=item * conflicts

=item * simulate

=item * verbose

=item * paranoid

=item * compat

=item * test_mode

=item * adopt

=item * no-folding

=item * ignore

=item * override

=item * defer

=back

N.B. This sets the current working directory to the target directory.

=cut

sub new {
    my $self = shift;
    my $class = ref($self) || $self;
    my %opts = @_;

    my $new = bless { }, $class;

    $new->{action_count} = 0;

    for my $required_arg (qw(dir target)) {
        croak "$class->new() called without '$required_arg' parameter\n"
            unless exists $opts{$required_arg};
        $new->{$required_arg} = delete $opts{$required_arg};
    }

    for my $opt (keys %DEFAULT_OPTIONS) {
        $new->{$opt} = exists $opts{$opt} ? delete $opts{$opt}
                                          : $DEFAULT_OPTIONS{$opt};
    }

    if (%opts) {
        croak "$class->new() called with unrecognised parameter(s): ",
            join(", ", keys %opts), "\n";
    }

    set_debug_level($new->get_verbosity());
    set_test_mode($new->{test_mode});
    $new->set_stow_dir();
    $new->init_state();

    return $new;
}

sub get_verbosity {
    my $self = shift;

    return $self->{verbose} unless $self->{test_mode};

    return 0 unless exists $ENV{TEST_VERBOSE};
    return 0 unless length $ENV{TEST_VERBOSE};

    # Convert TEST_VERBOSE=y into numeric value
    $ENV{TEST_VERBOSE} = 3 if $ENV{TEST_VERBOSE} !~ /^\d+$/;

    return $ENV{TEST_VERBOSE};
}

=head2 set_stow_dir([$dir])

Sets a new stow directory.  This allows the use of multiple stow
directories within one Stow instance, e.g.

    $stow->plan_stow('foo');
    $stow->set_stow_dir('/different/stow/dir');
    $stow->plan_stow('bar');
    $stow->process_tasks;

If C<$dir> is omitted, uses the value of the C<dir> parameter passed
to the L<new()> constructor.

=cut

sub set_stow_dir {
    my $self = shift;
    my ($dir) = @_;
    if (defined $dir) {
        $self->{dir} = $dir;
    }

    my $stow_dir = canon_path($self->{dir});
    my $target = canon_path($self->{target});
    $self->{stow_path} = File::Spec->abs2rel($stow_dir, $target);

    debug(2, "stow dir is $stow_dir");
    debug(2, "stow dir path relative to target $target is $self->{stow_path}");
}

sub init_state {
    my $self = shift;

    # Store conflicts during pre-processing
    $self->{conflicts}      = {};
    $self->{conflict_count} = 0;

    # Store command line packages to stow (-S and -R)
    $self->{pkgs_to_stow}   = [];

    # Store command line packages to unstow (-D and -R)
    $self->{pkgs_to_delete} = [];

    # The following structures are used by the abstractions that allow us to
    # defer operating on the filesystem until after all potential conflicts have
    # been assessed.

    # $self->{tasks}:  list of operations to be performed (in order)
    # each element is a hash ref of the form
    #   {
    #       action => ...  ('create' or 'remove' or 'move')
    #       type   => ...  ('link' or 'dir' or 'file')
    #       path   => ...  (unique)
    #       source => ...  (only for links)
    #       dest   => ...  (only for moving files)
    #   }
    $self->{tasks} = [];

    # $self->{dir_task_for}: map a path to the corresponding directory task reference
    # This structure allows us to quickly determine if a path has an existing
    # directory task associated with it.
    $self->{dir_task_for} = {};

    # $self->{link_task_for}: map a path to the corresponding directory task reference
    # This structure allows us to quickly determine if a path has an existing
    # directory task associated with it.
    $self->{link_task_for} = {};

    # N.B.: directory tasks and link tasks are NOT mutually exclusive due
    # to tree splitting (which involves a remove link task followed by
    # a create directory task).
}

=head1 METHODS

=head2 plan_unstow(@packages)

Plan which symlink/directory creation/removal tasks need to be executed
in order to unstow the given packages.  Any potential conflicts are then
accessible via L<get_conflicts()>.

=cut

sub plan_unstow {
    my $self = shift;
    my @packages = @_;

    $self->within_target_do(sub {
        for my $package (@packages) {
            my $path = join_paths($self->{stow_path}, $package);
            if (not -d $path) {
                error("The stow directory $self->{stow_path} does not contain package $package");
            }
            debug(2, "Planning unstow of package $package...");
            if ($self->{compat}) {
                $self->unstow_contents_orig(
                    $self->{stow_path},
                    $package,
                    '.',
                );
            }
            else {
                $self->unstow_contents(
                    $self->{stow_path},
                    $package,
                    '.',
                );
            }
            debug(2, "Planning unstow of package $package... done");
            $self->{action_count}++;
        }
    });
}

=head2 plan_stow(@packages)

Plan which symlink/directory creation/removal tasks need to be executed
in order to stow the given packages.  Any potential conflicts are then
accessible via L<get_conflicts()>.

=cut

sub plan_stow {
    my $self = shift;
    my @packages = @_;

    $self->within_target_do(sub {
        for my $package (@packages) {
            my $path = join_paths($self->{stow_path}, $package);
            if (not -d $path) {
                error("The stow directory $self->{stow_path} does not contain package $package");
            }
            debug(2, "Planning stow of package $package...");
            $self->stow_contents(
                $self->{stow_path},
                $package,
                '.',
                $path, # source from target
            );
            debug(2, "Planning stow of package $package... done");
            $self->{action_count}++;
        }
    });
}

#===== METHOD ===============================================================
# Name      : within_target_do()
# Purpose   : execute code within target directory, preserving cwd
# Parameters: $code => anonymous subroutine to execute within target dir
# Returns   : n/a
# Throws    : n/a
# Comments  : This is done to ensure that the consumer of the Stow interface
#           : doesn't have to worry about (a) what their cwd is, and
#           : (b) that their cwd might change.
#============================================================================
sub within_target_do {
    my $self = shift;
    my ($code) = @_;

    my $cwd = getcwd();
    chdir($self->{target})
        or error("Cannot chdir to target tree: $self->{target} ($!)");
    debug(3, "cwd now $self->{target}");

    $self->$code();

    restore_cwd($cwd);
    debug(3, "cwd restored to $cwd");
}

#===== METHOD ===============================================================
# Name      : stow_contents()
# Purpose   : stow the contents of the given directory
# Parameters: $stow_path => relative path from current (i.e. target) directory
#           :               to the stow dir containing the package to be stowed
#           : $package => the package whose contents are being stowed
#           : $target => subpath relative to package and target directories
#           : $source => relative path from the (sub)dir of target
#           :            to symlink source
# Returns   : n/a
# Throws    : a fatal error if directory cannot be read
# Comments  : stow_node() and stow_contents() are mutually recursive.
#           : $source and $target are used for creating the symlink
#           : $path is used for folding/unfolding trees as necessary
#============================================================================
sub stow_contents {
    my $self = shift;
    my ($stow_path, $package, $target, $source) = @_;

    my $path = join_paths($stow_path, $package, $target);

    return if $self->should_skip_target_which_is_stow_dir($target);

    my $cwd = getcwd();
    my $msg = "Stowing contents of $path (cwd=$cwd)";
    $msg =~ s!$ENV{HOME}(/|$)!~$1!g;
    debug(3, $msg);
    debug(4, "  => $source");

    error("stow_contents() called with non-directory path: $path")
        unless -d $path;
    error("stow_contents() called with non-directory target: $target")
        unless $self->is_a_node($target);

    opendir my $DIR, $path
        or error("cannot read directory: $path ($!)");
    my @listing = readdir $DIR;
    closedir $DIR;

    NODE:
    for my $node (@listing) {
        next NODE if $node eq '.';
        next NODE if $node eq '..';
        my $node_target = join_paths($target, $node);
        next NODE if $self->ignore($stow_path, $package, $node_target);
        $self->stow_node(
            $stow_path,
            $package,
            $node_target,                 # target
            join_paths($source, $node),   # source
        );
    }
}

#===== METHOD ===============================================================
# Name      : stow_node()
# Purpose   : stow the given node
# Parameters: $stow_path => relative path from current (i.e. target) directory
#           :               to the stow dir containing the node to be stowed
#           : $package => the package containing the node being stowed
#           : $target => subpath relative to package and target directories
#           : $source => relative path to symlink source from the dir of target
# Returns   : n/a
# Throws    : fatal exception if a conflict arises
# Comments  : stow_node() and stow_contents() are mutually recursive
#           : $source and $target are used for creating the symlink
#           : $path is used for folding/unfolding trees as necessary
#============================================================================
sub stow_node {
    my $self = shift;
    my ($stow_path, $package, $target, $source) = @_;

    my $path = join_paths($stow_path, $package, $target);

    debug(3, "Stowing $stow_path / $package / $target");
    debug(4, "  => $source");

    # Don't try to stow absolute symlinks (they can't be unstowed)
    if (-l $source) {
        my $second_source = $self->read_a_link($source);
        if ($second_source =~ m{\A/}) {
            $self->conflict(
                'stow',
                $package,
                "source is an absolute symlink $source => $second_source"
            );
            debug(3, "Absolute symlinks cannot be unstowed");
            return;
        }
    }

    # Does the target already exist?
    if ($self->is_a_link($target)) {
        # Where is the link pointing?
        my $existing_source = $self->read_a_link($target);
        if (not $existing_source) {
            error("Could not read link: $target");
        }
        debug(4, "  Evaluate existing link: $target => $existing_source");

        # Does it point to a node under any stow directory?
        my ($existing_path, $existing_stow_path, $existing_package) =
            $self->find_stowed_path($target, $existing_source);
        if (not $existing_path) {
            $self->conflict(
                'stow',
                $package,
                "existing target is not owned by stow: $target"
            );
            return; # XXX #
        }

        # Does the existing $target actually point to anything?
        if ($self->is_a_node($existing_path)) {
            if ($existing_source eq $source) {
                debug(2, "--- Skipping $target as it already points to $source");
            }
            elsif ($self->defer($target)) {
                debug(2, "--- Deferring installation of: $target");
            }
            elsif ($self->override($target)) {
                debug(2, "--- Overriding installation of: $target");
                $self->do_unlink($target);
                $self->do_link($source, $target);
            }
            elsif ($self->is_a_dir(join_paths(parent($target), $existing_source)) &&
                   $self->is_a_dir(join_paths(parent($target), $source))     ) {

                # If the existing link points to a directory,
                # and the proposed new link points to a directory,
                # then we can unfold (split open) the tree at that point

                debug(2, "--- Unfolding $target which was already owned by $existing_package");
                $self->do_unlink($target);
                $self->do_mkdir($target);
                $self->stow_contents(
                    $existing_stow_path,
                    $existing_package,
                    $target,
                    join_paths('..', $existing_source),
                );
                $self->stow_contents(
                    $self->{stow_path},
                    $package,
                    $target,
                    join_paths('..', $source),
                );
            }
            else {
                $self->conflict(
                    'stow',
                    $package,
                    "existing target is stowed to a different package: "
                    . "$target => $existing_source"
                );
            }
        }
        else {
            # The existing link is invalid, so replace it with a good link
            debug(2, "--- replacing invalid link: $path");
            $self->do_unlink($target);
            $self->do_link($source, $target);
        }
    }
    elsif ($self->is_a_node($target)) {
        debug(4, "  Evaluate existing node: $target");
        if ($self->is_a_dir($target)) {
            $self->stow_contents(
                $self->{stow_path},
                $package,
                $target,
                join_paths('..', $source),
            );
        }
        else {
            if ($self->{adopt}) {
                $self->do_mv($target, $path);
                $self->do_link($source, $target);
            }
            else {
                $self->conflict(
                    'stow',
                    $package,
                    "existing target is neither a link nor a directory: $target"
                );
            }
        }
    }
    elsif ($self->{'no-folding'} && -d $path && ! -l $path) {
        $self->do_mkdir($target);
        $self->stow_contents(
            $self->{stow_path},
            $package,
            $target,
            join_paths('..', $source),
        );
    }
    else {
        $self->do_link($source, $target);
    }
    return;
}

#===== METHOD ===============================================================
# Name      : should_skip_target_which_is_stow_dir()
# Purpose   : determine whether target is a stow directory which should
#           : not be stowed to or unstowed from
# Parameters: $target => relative path to symlink target from the current directory
# Returns   : true iff target is a stow directory
# Throws    : n/a
# Comments  : none
#============================================================================
sub should_skip_target_which_is_stow_dir {
    my $self = shift;
    my ($target) = @_;

    # Don't try to remove anything under a stow directory
    if ($target eq $self->{stow_path}) {
        warn "WARNING: skipping target which was current stow directory $target\n";
        return 1;
    }

    if ($self->marked_stow_dir($target)) {
        warn "WARNING: skipping protected directory $target\n";
        return 1;
    }

    debug (4, "$target not protected");
    return 0;
}

sub marked_stow_dir {
    my $self = shift;
    my ($target) = @_;
    for my $f (".stow", ".nonstow") {
        if (-e join_paths($target, $f)) {
            debug(4, "$target contained $f");
            return 1;
        }
    }
    return 0;
}

#===== METHOD ===============================================================
# Name      : unstow_contents_orig()
# Purpose   : unstow the contents of the given directory
# Parameters: $stow_path => relative path from current (i.e. target) directory
#           :               to the stow dir containing the package to be unstowed
#           : $package => the package whose contents are being unstowed
#           : $target => relative path to symlink target from the current directory
# Returns   : n/a
# Throws    : a fatal error if directory cannot be read
# Comments  : unstow_node_orig() and unstow_contents_orig() are mutually recursive
#           : Here we traverse the target tree, rather than the source tree.
#============================================================================
sub unstow_contents_orig {
    my $self = shift;
    my ($stow_path, $package, $target) = @_;

    my $path = join_paths($stow_path, $package, $target);

    return if $self->should_skip_target_which_is_stow_dir($target);

    my $cwd = getcwd();
    my $msg = "Unstowing from $target (compat mode, cwd=$cwd, stow dir=$self->{stow_path})";
    $msg =~ s!$ENV{HOME}(/|$)!~$1!g;
    debug(3, $msg);
    debug(4, "  source path is $path");
    # In compat mode we traverse the target tree not the source tree,
    # so we're unstowing the contents of /target/foo, there's no
    # guarantee that the corresponding /stow/mypkg/foo exists.
    error("unstow_contents_orig() called with non-directory target: $target")
        unless -d $target;

    opendir my $DIR, $target
        or error("cannot read directory: $target ($!)");
    my @listing = readdir $DIR;
    closedir $DIR;

    NODE:
    for my $node (@listing) {
        next NODE if $node eq '.';
        next NODE if $node eq '..';
        my $node_target = join_paths($target, $node);
        next NODE if $self->ignore($stow_path, $package, $node_target);
        $self->unstow_node_orig($stow_path, $package, $node_target);
    }
}

#===== METHOD ===============================================================
# Name      : unstow_node_orig()
# Purpose   : unstow the given node
# Parameters: $stow_path => relative path from current (i.e. target) directory
#           :               to the stow dir containing the node to be stowed
#           : $package => the package containing the node being stowed
#           : $target => relative path to symlink target from the current directory
# Returns   : n/a
# Throws    : fatal error if a conflict arises
# Comments  : unstow_node() and unstow_contents() are mutually recursive
#============================================================================
sub unstow_node_orig {
    my $self = shift;
    my ($stow_path, $package, $target) = @_;

    my $path = join_paths($stow_path, $package, $target);

    debug(3, "Unstowing $target (compat mode)");
    debug(4, "  source path is $path");

    # Does the target exist?
    if ($self->is_a_link($target)) {
        debug(4, "  Evaluate existing link: $target");

        # Where is the link pointing?
        my $existing_source = $self->read_a_link($target);
        if (not $existing_source) {
            error("Could not read link: $target");
        }

        # Does it point to a node under any stow directory?
        my ($existing_path, $existing_stow_path, $existing_package) =
            $self->find_stowed_path($target, $existing_source);
        if (not $existing_path) {
            # We're traversing the target tree not the package tree,
            # so we definitely expect to find stuff not owned by stow.
            # Therefore we can't flag a conflict.
            return; # XXX #
        }

        # Does the existing $target actually point to anything?
        if (-e $existing_path) {
            # Does link point to the right place?
            if ($existing_path eq $path) {
                $self->do_unlink($target);
            }
            elsif ($self->override($target)) {
                debug(2, "--- overriding installation of: $target");
                $self->do_unlink($target);
            }
            # else leave it alone
        }
        else {
            debug(2, "--- removing invalid link into a stow directory: $path");
            $self->do_unlink($target);
        }
    }
    elsif (-d $target) {
        $self->unstow_contents_orig($stow_path, $package, $target);

        # This action may have made the parent directory foldable
        if (my $parent = $self->foldable($target)) {
            $self->fold_tree($target, $parent);
        }
    }
    elsif (-e $target) {
        $self->conflict(
            'unstow',
            $package,
            "existing target is neither a link nor a directory: $target",
        );
    }
    else {
        debug(2, "$target did not exist to be unstowed");
    }
    return;
}

#===== METHOD ===============================================================
# Name      : unstow_contents()
# Purpose   : unstow the contents of the given directory
# Parameters: $stow_path => relative path from current (i.e. target) directory
#           :               to the stow dir containing the package to be unstowed
#           : $package => the package whose contents are being unstowed
#           : $target => relative path to symlink target from the current directory
# Returns   : n/a
# Throws    : a fatal error if directory cannot be read
# Comments  : unstow_node() and unstow_contents() are mutually recursive
#           : Here we traverse the source tree, rather than the target tree.
#============================================================================
sub unstow_contents {
    my $self = shift;
    my ($stow_path, $package, $target) = @_;

    my $path = join_paths($stow_path, $package, $target);

    return if $self->should_skip_target_which_is_stow_dir($target);

    my $cwd = getcwd();
    my $msg = "Unstowing from $target (cwd=$cwd, stow dir=$self->{stow_path})";
    $msg =~ s!$ENV{HOME}/!~/!g;
    debug(3, $msg);
    debug(4, "  source path is $path");
    # We traverse the source tree not the target tree, so $path must exist.
    error("unstow_contents() called with non-directory path: $path")
        unless -d $path;
    # When called at the top level, $target should exist.  And
    # unstow_node() should only call this via mutual recursion if
    # $target exists.
    error("unstow_contents() called with invalid target: $target")
        unless $self->is_a_node($target);

    opendir my $DIR, $path
        or error("cannot read directory: $path ($!)");
    my @listing = readdir $DIR;
    closedir $DIR;

    NODE:
    for my $node (@listing) {
        next NODE if $node eq '.';
        next NODE if $node eq '..';
        my $node_target = join_paths($target, $node);
        next NODE if $self->ignore($stow_path, $package, $node_target);
        $self->unstow_node($stow_path, $package, $node_target);
    }
    if (-d $target) {
        $self->cleanup_invalid_links($target);
    }
}

#===== METHOD ===============================================================
# Name      : unstow_node()
# Purpose   : unstow the given node
# Parameters: $stow_path => relative path from current (i.e. target) directory
#           :               to the stow dir containing the node to be stowed
#           : $package => the package containing the node being unstowed
#           : $target => relative path to symlink target from the current directory
# Returns   : n/a
# Throws    : fatal error if a conflict arises
# Comments  : unstow_node() and unstow_contents() are mutually recursive
#============================================================================
sub unstow_node {
    my $self = shift;
    my ($stow_path, $package, $target) = @_;

    my $path = join_paths($stow_path, $package, $target);

    debug(3, "Unstowing $path");
    debug(4, "  target is $target");

    # Does the target exist?
    if ($self->is_a_link($target)) {
        debug(4, "  Evaluate existing link: $target");

        # Where is the link pointing?
        my $existing_source = $self->read_a_link($target);
        if (not $existing_source) {
            error("Could not read link: $target");
        }

        if ($existing_source =~ m{\A/}) {
            warn "Ignoring an absolute symlink: $target => $existing_source\n";
            return; # XXX #
        }

        # Does it point to a node under any stow directory?
        my ($existing_path, $existing_stow_path, $existing_package) =
            $self->find_stowed_path($target, $existing_source);
        if (not $existing_path) {
             $self->conflict(
                 'unstow',
                 $package,
                 "existing target is not owned by stow: $target => $existing_source"
             );
            return; # XXX #
        }

        # Does the existing $target actually point to anything?
        if (-e $existing_path) {
            # Does link points to the right place?
            if ($existing_path eq $path) {
                $self->do_unlink($target);
            }

            # XXX we quietly ignore links that are stowed to a different
            # package.

            #elsif (defer($target)) {
            #    debug(2, "--- deferring to installation of: $target");
            #}
            #elsif ($self->override($target)) {
            #    debug(2, "--- overriding installation of: $target");
            #    $self->do_unlink($target);
            #}
            #else {
            #    $self->conflict(
            #        'unstow',
            #        $package,
            #        "existing target is stowed to a different package: "
            #        . "$target => $existing_source"
            #    );
            #}
        }
        else {
            debug(2, "--- removing invalid link into a stow directory: $path");
            $self->do_unlink($target);
        }
    }
    elsif (-e $target) {
        debug(4, "  Evaluate existing node: $target");
        if (-d $target) {
            $self->unstow_contents($stow_path, $package, $target);

            # This action may have made the parent directory foldable
            if (my $parent = $self->foldable($target)) {
                $self->fold_tree($target, $parent);
            }
        }
        else {
            $self->conflict(
                'unstow',
                $package,
                "existing target is neither a link nor a directory: $target",
            );
        }
    }
    else {
        debug(2, "$target did not exist to be unstowed");
    }
    return;
}

#===== METHOD ===============================================================
# Name      : path_owned_by_package()
# Purpose   : determine whether the given link points to a member of a
#           : stowed package
# Parameters: $target => path to a symbolic link under current directory
#           : $source => where that link points to
# Returns   : the package iff link is owned by stow, otherwise ''
# Throws    : n/a
# Comments  : lossy wrapper around find_stowed_path()
#============================================================================
sub path_owned_by_package {
    my $self = shift;
    my ($target, $source) = @_;

    my ($path, $stow_path, $package) =
        $self->find_stowed_path($target, $source);
    return $package;
}

#===== METHOD ===============================================================
# Name      : find_stowed_path()
# Purpose   : determine whether the given link points to a member of a
#           : stowed package
# Parameters: $target => path to a symbolic link under current directory
#           : $source => where that link points to (needed because link
#           :            might not exist yet due to two-phase approach,
#           :            so we can't just call readlink())
# Returns   : ($path, $stow_path, $package) where $path and $stow_path are
#           : relative from the current (i.e. target) directory.  $path
#           : is the full relative path, $stow_path is the relative path
#           : to the stow directory, and $package is the name of the package.
#           : or ('', '', '') if link is not owned by stow
# Throws    : n/a
# Comments  : Needs 
#           : Allow for stow dir not being under target dir.
#           : We could put more logic under here for multiple stow dirs.
#============================================================================
sub find_stowed_path {
    my $self = shift;
    my ($target, $source) = @_;

    # Evaluate softlink relative to its target
    my $path = join_paths(parent($target), $source);
    debug(4, "  is path $path owned by stow?");

    # Search for .stow files - this allows us to detect links
    # owned by stow directories other than the current one.
    my $dir = '';
    my @path = split m{/+}, $path;
    for my $i (0 .. $#path) {
        my $part = $path[$i];
        $dir = join_paths($dir, $part);
        if ($self->marked_stow_dir($dir)) {
            # FIXME - not sure if this can ever happen
            internal_error("find_stowed_path() called directly on stow dir")
                if $i == $#path;

            debug(4, "    yes - $dir was marked as a stow dir");
            my $package = $path[$i + 1];
            return ($path, $dir, $package);
        }
    }

    # If no .stow file was found, we need to find out whether it's
    # owned by the current stow directory, in which case $path will be
    # a prefix of $self->{stow_path}.
    my @stow_path = split m{/+}, $self->{stow_path};

    # Strip off common prefixes until one is empty
    while (@path && @stow_path) {
        if ((shift @path) ne (shift @stow_path)) {
            debug(4, "    no - either $path not under $self->{stow_path} or vice-versa");
            return ('', '', '');
        }
    }

    if (@stow_path) { # @path must be empty
        debug(4, "    no - $path is not under $self->{stow_path}");
        return ('', '', '');
    }

    my $package = shift @path;
    
    debug(4, "    yes - by $package in " . join_paths(@path));
    return ($path, $self->{stow_path}, $package);
}

#===== METHOD ================================================================
# Name      : cleanup_invalid_links()
# Purpose   : clean up invalid links that may block folding
# Parameters: $dir => path to directory to check
# Returns   : n/a
# Throws    : no exceptions
# Comments  : removing files from a stowed package is probably a bad practice
#           : so this kind of clean up is not _really_ stow's responsibility;
#           : however, failing to clean up can block tree folding, so we'll do
#           : it anyway
#=============================================================================
sub cleanup_invalid_links {
    my $self = shift;
    my ($dir) = @_;

    if (not -d $dir) {
        error("cleanup_invalid_links() called with a non-directory: $dir");
    }

    opendir my $DIR, $dir
        or error("cannot read directory: $dir ($!)");
    my @listing = readdir $DIR;
    closedir $DIR;

    NODE:
    for my $node (@listing) {
        next NODE if $node eq '.';
        next NODE if $node eq '..';

        my $node_path = join_paths($dir, $node);

        if (-l $node_path and not exists $self->{link_task_for}{$node_path}) {

            # Where is the link pointing?
            # (don't use read_a_link() here)
            my $source = readlink($node_path);
            if (not $source) {
                error("Could not read link $node_path");
            }

            if (
                not -e join_paths($dir, $source) and  # bad link
                $self->path_owned_by_package($node_path, $source) # owned by stow
            ){
                debug(2, "--- removing stale link: $node_path => " .
                          join_paths($dir, $source));
                $self->do_unlink($node_path);
            }
        }
    }
    return;
}


#===== METHOD ===============================================================
# Name      : foldable()
# Purpose   : determine whether a tree can be folded
# Parameters: $target => path to a directory
# Returns   : path to the parent dir iff the tree can be safely folded
# Throws    : n/a
# Comments  : the path returned is relative to the parent of $target,
#           : that is, it can be used as the source for a replacement symlink
#============================================================================
sub foldable {
    my $self = shift;
    my ($target) = @_;

    debug(3, "--- Is $target foldable?");
    if ($self->{'no-folding'}) {
        debug(3, "--- no because --no-folding enabled");
        return '';
    }

    opendir my $DIR, $target
        or error(qq{Cannot read directory "$target" ($!)\n});
    my @listing = readdir $DIR;
    closedir $DIR;

    my $parent = '';
    NODE:
    for my $node (@listing) {

        next NODE if $node eq '.';
        next NODE if $node eq '..';

        my $path =  join_paths($target, $node);

        # Skip nodes scheduled for removal
        next NODE if not $self->is_a_node($path);

        # If it's not a link then we can't fold its parent
        return '' if not $self->is_a_link($path);

        # Where is the link pointing?
        my $source = $self->read_a_link($path);
        if (not $source) {
            error("Could not read link $path");
        }
        if ($parent eq '') {
            $parent = parent($source)
        }
        elsif ($parent ne parent($source)) {
            return '';
        }
    }
    return '' if not $parent;

    # If we get here then all nodes inside $target are links, and those links
    # point to nodes inside the same directory.

    # chop of leading '..' to get the path to the common parent directory
    # relative to the parent of our $target
    $parent =~ s{\A\.\./}{};

    # If the resulting path is owned by stow, we can fold it
    if ($self->path_owned_by_package($target, $parent)) {
        debug(3, "--- $target is foldable");
        return $parent;
    }
    else {
        return '';
    }
}

#===== METHOD ===============================================================
# Name      : fold_tree()
# Purpose   : fold the given tree
# Parameters: $source  => link to the folded tree source
#           : $target => directory that we will replace with a link to $source
# Returns   : n/a
# Throws    : none
# Comments  : only called iff foldable() is true so we can remove some checks
#============================================================================
sub fold_tree {
    my $self = shift;
    my ($target, $source) = @_;

    debug(3, "--- Folding tree: $target => $source");

    opendir my $DIR, $target
        or error(qq{Cannot read directory "$target" ($!)\n});
    my @listing = readdir $DIR;
    closedir $DIR;

    NODE:
    for my $node (@listing) {
        next NODE if $node eq '.';
        next NODE if $node eq '..';
        next NODE if not $self->is_a_node(join_paths($target, $node));
        $self->do_unlink(join_paths($target, $node));
    }
    $self->do_rmdir($target);
    $self->do_link($source, $target);
    return;
}


#===== METHOD ===============================================================
# Name      : conflict()
# Purpose   : handle conflicts in stow operations
# Parameters: $package => the package involved with the conflicting operation
#           : $message => a description of the conflict
# Returns   : n/a
# Throws    : none
# Comments  : none
#============================================================================
sub conflict {
    my $self = shift;
    my ($action, $package, $message) = @_;

    debug(2, "CONFLICT when ${action}ing $package: $message");
    $self->{conflicts}{$action}{$package} ||= [];
    push @{ $self->{conflicts}{$action}{$package} }, $message;
    $self->{conflict_count}++;

    return;
}

=head2 get_conflicts()

Returns a nested hash of all potential conflicts discovered: the keys
are actions ('stow' or 'unstow'), and the values are hashrefs whose
keys are stow package names and whose values are conflict
descriptions, e.g.:

    (
        stow => {
            perl => [
                "existing target is not owned by stow: bin/a2p"
                "existing target is neither a link nor a directory: bin/perl"
            ]
        }
    )

=cut

sub get_conflicts {
    my $self = shift;
    return %{ $self->{conflicts} };
}

=head2 get_conflict_count()

Returns the number of conflicts found.

=cut

sub get_conflict_count {
    my $self = shift;
    return $self->{conflict_count};
}

=head2 get_tasks()

Returns a list of all symlink/directory creation/removal tasks.

=cut

sub get_tasks {
    my $self = shift;
    return @{ $self->{tasks} };
}

=head2 get_action_count()

Returns the number of actions planned for this Stow instance.

=cut

sub get_action_count {
    my $self = shift;
    return $self->{action_count};
}

#===== METHOD ================================================================
# Name      : ignore
# Purpose   : determine if the given path matches a regex in our ignore list
# Parameters: $stow_path => the stow directory containing the package
#           : $package   => the package containing the path
#           : $target    => the path to check against the ignore list
#           :               relative to its package directory
# Returns   : true iff the path should be ignored
# Throws    : no exceptions
# Comments  : none
#=============================================================================
sub ignore {
    my $self = shift;
    my ($stow_path, $package, $target) = @_;

    internal_error(__PACKAGE__ . "::ignore() called with empty target")
        unless length $target;

    for my $suffix (@{ $self->{ignore} }) {
        if ($target =~ m/$suffix/) {
            debug(4, "  Ignoring path $target due to --ignore=$suffix");
            return 1;
        }
    }

    my $package_dir = join_paths($stow_path, $package);
    my ($path_regexp, $segment_regexp) =
        $self->get_ignore_regexps($package_dir);
    debug(5, "    Ignore list regexp for paths:    " .
             (defined $path_regexp ? "/$path_regexp/" : "none"));
    debug(5, "    Ignore list regexp for segments: " .
             (defined $segment_regexp ? "/$segment_regexp/" : "none"));

    if (defined $path_regexp and "/$target" =~ $path_regexp) {
        debug(4, "  Ignoring path /$target");
        return 1;
    }

    (my $basename = $target) =~ s!.+/!!;
    if (defined $segment_regexp and $basename =~ $segment_regexp) {
        debug(4, "  Ignoring path segment $basename");
        return 1;
    }

    debug(5, "  Not ignoring $target");
    return 0;
}

sub get_ignore_regexps {
    my $self = shift;
    my ($dir) = @_;

    # N.B. the local and global stow ignore files have to have different
    # names so that:
    #   1. the global one can be a symlink to within a stow
    #      package, managed by stow itself, and
    #   2. the local ones can be ignored via hardcoded logic in
    #      GlobsToRegexp(), so that they always stay within their stow packages.

    my $local_stow_ignore  = join_paths($dir,       $LOCAL_IGNORE_FILE);
    my $global_stow_ignore = join_paths($ENV{HOME}, $GLOBAL_IGNORE_FILE);

    for my $file ($local_stow_ignore, $global_stow_ignore) {
        if (-e $file) {
            debug(5, "  Using ignore file: $file");
            return $self->get_ignore_regexps_from_file($file);
        }
        else {
            debug(5, "  $file didn't exist");
        }
    }

    debug(4, "  Using built-in ignore list");
    return @default_global_ignore_regexps;
}

my %ignore_file_regexps;

sub get_ignore_regexps_from_file {
    my $self = shift;
    my ($file) = @_;

    if (exists $ignore_file_regexps{$file}) {
        debug(4, "    Using memoized regexps from $file");
        return @{ $ignore_file_regexps{$file} };
    }

    if (! open(REGEXPS, $file)) {
        debug(4, "    Failed to open $file: $!");
        return undef;
    }

    my @regexps = $self->get_ignore_regexps_from_fh(\*REGEXPS);
    close(REGEXPS);

    $ignore_file_regexps{$file} = [ @regexps ];
    return @regexps;
}

=head2 invalidate_memoized_regexp($file)

For efficiency of performance, regular expressions are compiled from
each ignore list file the first time it is used by the Stow process,
and then memoized for future use.  If you expect the contents of these
files to change during a single run, you will need to invalidate the
memoized value from this cache.  This method allows you to do that.

=cut

sub invalidate_memoized_regexp {
    my $self = shift;
    my ($file) = @_;
    if (exists $ignore_file_regexps{$file}) {
        debug(4, "    Invalidated memoized regexp for $file");
        delete $ignore_file_regexps{$file};
    }
    else {
        debug(2, "  WARNING: no memoized regexp for $file to invalidate");
    }
}

sub get_ignore_regexps_from_fh {
    my $self = shift;
    my ($fh) = @_;
    my %regexps;
    while (<$fh>) {
        chomp;
        s/^\s+//;
        s/\s+$//;
        next if /^#/ or length($_) == 0;
        s/\s+#.+//; # strip comments to right of pattern
        s/\\#/#/g;
        $regexps{$_}++;
    }

    # Local ignore lists should *always* stay within the stow directory,
    # because this is the only place stow looks for them.
    $regexps{"^/\Q$LOCAL_IGNORE_FILE\E\$"}++;

    return $self->compile_ignore_regexps(%regexps);
}

sub compile_ignore_regexps {
    my $self = shift;
    my (%regexps) = @_;

    my @segment_regexps;
    my @path_regexps;
    for my $regexp (keys %regexps) {
        if (index($regexp, '/') < 0) {
            # No / found in regexp, so use it for matching against basename
            push @segment_regexps, $regexp;
        }
        else {
            # / found in regexp, so use it for matching against full path
            push @path_regexps, $regexp;
        }
    }

    my $segment_regexp = join '|', @segment_regexps;
    my $path_regexp    = join '|', @path_regexps;
    $segment_regexp = @segment_regexps ?
        $self->compile_regexp("^($segment_regexp)\$") : undef;
    $path_regexp    = @path_regexps    ?
        $self->compile_regexp("(^|/)($path_regexp)(/|\$)") : undef;

    return ($path_regexp, $segment_regexp);
}

sub compile_regexp {
    my $self = shift;
    my ($regexp) = @_;
    my $compiled = eval { qr/$regexp/ };
    die "Failed to compile regexp: $@\n" if $@;
    return $compiled;
}

sub get_default_global_ignore_regexps {
    my $class = shift;
    # Bootstrap issue - first time we stow, we will be stowing
    # .cvsignore so it might not exist in ~ yet, or if it does, it could
    # be an old version missing the entries we need.  So we make sure
    # they are there by hardcoding some crucial entries.
    return $class->get_ignore_regexps_from_fh(\*DATA);
}

#===== METHOD ================================================================
# Name      : defer
# Purpose   : determine if the given path matches a regex in our defer list
# Parameters: $path
# Returns   : Boolean
# Throws    : no exceptions
# Comments  : none
#=============================================================================
sub defer {
    my $self = shift;
    my ($path) = @_;

    for my $prefix (@{ $self->{defer} }) {
        return 1 if $path =~ m/$prefix/;
    }
    return 0;
}

#===== METHOD ================================================================
# Name      : override
# Purpose   : determine if the given path matches a regex in our override list
# Parameters: $path
# Returns   : Boolean
# Throws    : no exceptions
# Comments  : none
#=============================================================================
sub override {
    my $self = shift;
    my ($path) = @_;

    for my $regex (@{ $self->{override} }) {
        return 1 if $path =~ m/$regex/;
    }
    return 0;
}

##############################################################################
#
# The following code provides the abstractions that allow us to defer operating
# on the filesystem until after all potential conflcits have been assessed.
#
##############################################################################

#===== METHOD ===============================================================
# Name      : process_tasks()
# Purpose   : process each task in the tasks list
# Parameters: none
# Returns   : n/a
# Throws    : fatal error if tasks list is corrupted or a task fails
# Comments  : none
#============================================================================
sub process_tasks {
    my $self = shift;

    debug(2, "Processing tasks...");

    # Strip out all tasks with a skip action
    $self->{tasks} = [ grep { $_->{action} ne 'skip' } @{ $self->{tasks} } ];

    if (not @{ $self->{tasks} }) {
        return;
    }

    $self->within_target_do(sub {
        for my $task (@{ $self->{tasks} }) {
            $self->process_task($task);
        }
    });

    debug(2, "Processing tasks... done");
}

#===== METHOD ===============================================================
# Name      : process_task()
# Purpose   : process a single task
# Parameters: $task => the task to process
# Returns   : n/a
# Throws    : fatal error if task fails
# Comments  : Must run from within target directory.
#           : Task involve either creating or deleting dirs and symlinks
#           : an action is set to 'skip' if it is found to be redundant
#============================================================================
sub process_task {
    my $self = shift;
    my ($task) = @_;

    if ($task->{action} eq 'create') {
        if ($task->{type} eq 'dir') {
            mkdir($task->{path}, 0777)
                or error("Could not create directory: $task->{path} ($!)");
            return;
        }
        elsif ($task->{type} eq 'link') {
            symlink $task->{source}, $task->{path}
                or error(
                    "Could not create symlink: %s => %s ($!)",
                    $task->{path},
                    $task->{source}
            );
            return;
        }
    }
    elsif ($task->{action} eq 'remove') {
        if ($task->{type} eq 'dir') {
            rmdir $task->{path}
                or error("Could not remove directory: $task->{path} ($!)");
            return;
        }
        elsif ($task->{type} eq 'link') {
            unlink $task->{path}
                or error("Could not remove link: $task->{path} ($!)");
            return;
        }
    }
    elsif ($task->{action} eq 'move') {
        if ($task->{type} eq 'file') {
            # rename() not good enough, since the stow directory
            # might be on a different filesystem to the target.
            move $task->{path}, $task->{dest}
                or error("Could not move $task->{path} -> $task->{dest} ($!)");
            return;
        }
    }

    # Should never happen.
    internal_error("bad task action: $task->{action}");
}

#===== METHOD ===============================================================
# Name      : link_task_action()
# Purpose   : finds the link task action for the given path, if there is one
# Parameters: $path
# Returns   : 'remove', 'create', or '' if there is no action
# Throws    : a fatal exception if an invalid action is found
# Comments  : none
#============================================================================
sub link_task_action {
    my $self = shift;
    my ($path) = @_;

    if (! exists $self->{link_task_for}{$path}) {
        debug(4, "  link_task_action($path): no task");
        return '';
    }

    my $action = $self->{link_task_for}{$path}->{action};
    internal_error("bad task action: $action")
        unless $action eq 'remove' or $action eq 'create';

    debug(4, "  link_task_action($path): link task exists with action $action");
    return $action;
}

#===== METHOD ===============================================================
# Name      : dir_task_action()
# Purpose   : finds the dir task action for the given path, if there is one
# Parameters: $path
# Returns   : 'remove', 'create', or '' if there is no action
# Throws    : a fatal exception if an invalid action is found
# Comments  : none
#============================================================================
sub dir_task_action {
    my $self = shift;
    my ($path) = @_;

    if (! exists $self->{dir_task_for}{$path}) {
        debug(4, "  dir_task_action($path): no task");
        return '';
    }

    my $action = $self->{dir_task_for}{$path}->{action};
    internal_error("bad task action: $action")
        unless $action eq 'remove' or $action eq 'create';

    debug(4, "  dir_task_action($path): dir task exists with action $action");
    return $action;
}

#===== METHOD ===============================================================
# Name      : parent_link_scheduled_for_removal()
# Purpose   : determine whether the given path or any parent thereof
#           : is a link scheduled for removal
# Parameters: $path
# Returns   : Boolean
# Throws    : none
# Comments  : none
#============================================================================
sub parent_link_scheduled_for_removal {
    my $self = shift;
    my ($path) = @_;

    my $prefix = '';
    for my $part (split m{/+}, $path) {
        $prefix = join_paths($prefix, $part);
        debug(4, "    parent_link_scheduled_for_removal($path): prefix $prefix");
        if (exists $self->{link_task_for}{$prefix} and
             $self->{link_task_for}{$prefix}->{action} eq 'remove') {
            debug(4, "    parent_link_scheduled_for_removal($path): link scheduled for removal");
            return 1;
        }
    }

    debug(4, "    parent_link_scheduled_for_removal($path): returning false");
    return 0;
}

#===== METHOD ===============================================================
# Name      : is_a_link()
# Purpose   : determine if the given path is a current or planned link
# Parameters: $path
# Returns   : Boolean
# Throws    : none
# Comments  : returns false if an existing link is scheduled for removal
#           : and true if a non-existent link is scheduled for creation
#============================================================================
sub is_a_link {
    my $self = shift;
    my ($path) = @_;
    debug(4, "  is_a_link($path)");

    if (my $action = $self->link_task_action($path)) {
        if ($action eq 'remove') {
            debug(4, "  is_a_link($path): returning 0 (remove action found)");
            return 0;
        }
        elsif ($action eq 'create') {
            debug(4, "  is_a_link($path): returning 1 (create action found)");
            return 1;
        }
    }

    if (-l $path) {
        # Check if any of its parent are links scheduled for removal
        # (need this for edge case during unfolding)
        debug(4, "  is_a_link($path): is a real link");
        return $self->parent_link_scheduled_for_removal($path) ? 0 : 1;
    }

    debug(4, "  is_a_link($path): returning 0");
    return 0;
}

#===== METHOD ===============================================================
# Name      : is_a_dir()
# Purpose   : determine if the given path is a current or planned directory
# Parameters: $path
# Returns   : Boolean
# Throws    : none
# Comments  : returns false if an existing directory is scheduled for removal
#           : and true if a non-existent directory is scheduled for creation
#           : we also need to be sure we are not just following a link
#============================================================================
sub is_a_dir {
    my $self = shift;
    my ($path) = @_;
    debug(4, "  is_a_dir($path)");

    if (my $action = $self->dir_task_action($path)) {
        if ($action eq 'remove') {
            return 0;
        }
        elsif ($action eq 'create') {
            return 1;
        }
    }

    return 0 if $self->parent_link_scheduled_for_removal($path);

    if (-d $path) {
        debug(4, "  is_a_dir($path): real dir");
        return 1;
    }

    debug(4, "  is_a_dir($path): returning false");
    return 0;
}

#===== METHOD ===============================================================
# Name      : is_a_node()
# Purpose   : determine whether the given path is a current or planned node
# Parameters: $path
# Returns   : Boolean
# Throws    : none
# Comments  : returns false if an existing node is scheduled for removal
#           : true if a non-existent node is scheduled for creation
#           : we also need to be sure we are not just following a link
#============================================================================
sub is_a_node {
    my $self = shift;
    my ($path) = @_;
    debug(4, "  is_a_node($path)");

    my $laction = $self->link_task_action($path);
    my $daction = $self->dir_task_action($path);

    if ($laction eq 'remove') {
        if ($daction eq 'remove') {
            internal_error("removing link and dir: $path");
            return 0;
        }
        elsif ($daction eq 'create') {
            # Assume that we're unfolding $path, and that the link
            # removal action is earlier than the dir creation action
            # in the task queue.  FIXME: is this a safe assumption?
            return 1;
        }
        else { # no dir action
            return 0;
        }
    }
    elsif ($laction eq 'create') {
        if ($daction eq 'remove') {
            # Assume that we're folding $path, and that the dir
            # removal action is earlier than the link creation action
            # in the task queue.  FIXME: is this a safe assumption?
            return 1;
        }
        elsif ($daction eq 'create') {
            internal_error("creating link and dir: $path");
            return 1;
        }
        else { # no dir action
            return 1;
        }
    }
    else {
        # No link action
        if ($daction eq 'remove') {
            return 0;
        }
        elsif ($daction eq 'create') {
            return 1;
        }
        else { # no dir action
            # fall through to below
        }
    }

    return 0 if $self->parent_link_scheduled_for_removal($path);

    if (-e $path) {
        debug(4, "  is_a_node($path): really exists");
        return 1;
    }

    debug(4, "  is_a_node($path): returning false");
    return 0;
}

#===== METHOD ===============================================================
# Name      : read_a_link()
# Purpose   : return the source of a current or planned link
# Parameters: $path => path to the link target
# Returns   : a string
# Throws    : fatal exception if the given path is not a current or planned
#           : link
# Comments  : none
#============================================================================
sub read_a_link {
    my $self = shift;
    my ($path) = @_;

    if (my $action = $self->link_task_action($path)) {
        debug(4, "  read_a_link($path): task exists with action $action");

        if ($action eq 'create') {
            return $self->{link_task_for}{$path}->{source};
        }
        elsif ($action eq 'remove') {
            internal_error(
                "read_a_link() passed a path that is scheduled for removal: $path"
            );
        }
    }
    elsif (-l $path) {
        debug(4, "  read_a_link($path): real link");
        my $target = readlink $path or error("Could not read link: $path ($!)");
        return $target;
    }
    internal_error("read_a_link() passed a non link path: $path\n");
}

#===== METHOD ===============================================================
# Name      : do_link()
# Purpose   : wrap 'link' operation for later processing
# Parameters: $oldfile => the existing file to link to
#           : $newfile => the file to link
# Returns   : n/a
# Throws    : error if this clashes with an existing planned operation
# Comments  : cleans up operations that undo previous operations
#============================================================================
sub do_link {
    my $self = shift;
    my ($oldfile, $newfile) = @_;

    if (exists $self->{dir_task_for}{$newfile}) {
        my $task_ref = $self->{dir_task_for}{$newfile};

        if ($task_ref->{action} eq 'create') {
            if ($task_ref->{type} eq 'dir') {
                internal_error(
                    "new link (%s => %s) clashes with planned new directory",
                    $newfile,
                    $oldfile,
                );
            }
        }
        elsif ($task_ref->{action} eq 'remove') {
            # We may need to remove a directory before creating a link so continue.
        }
        else {
            internal_error("bad task action: $task_ref->{action}");
        }
    }

    if (exists $self->{link_task_for}{$newfile}) {
        my $task_ref = $self->{link_task_for}{$newfile};

        if ($task_ref->{action} eq 'create') {
            if ($task_ref->{source} ne $oldfile) {
                internal_error(
                    "new link clashes with planned new link: %s => %s",
                    $task_ref->{path},
                    $task_ref->{source},
                )
            }
            else {
                debug(1, "LINK: $newfile => $oldfile (duplicates previous action)");
                return;
            }
        }
        elsif ($task_ref->{action} eq 'remove') {
            if ($task_ref->{source} eq $oldfile) {
                # No need to remove a link we are going to recreate
                debug(1, "LINK: $newfile => $oldfile (reverts previous action)");
                $self->{link_task_for}{$newfile}->{action} = 'skip';
                delete $self->{link_task_for}{$newfile};
                return;
            }
            # We may need to remove a link to replace it so continue
        }
        else {
            internal_error("bad task action: $task_ref->{action}");
        }
    }

    # Creating a new link
    debug(1, "LINK: $newfile => $oldfile");
    my $task = {
        action  => 'create',
        type    => 'link',
        path    => $newfile,
        source  => $oldfile,
    };
    push @{ $self->{tasks} }, $task;
    $self->{link_task_for}{$newfile} = $task;

    return;
}

#===== METHOD ===============================================================
# Name      : do_unlink()
# Purpose   : wrap 'unlink' operation for later processing
# Parameters: $file => the file to unlink
# Returns   : n/a
# Throws    : error if this clashes with an existing planned operation
# Comments  : will remove an existing planned link
#============================================================================
sub do_unlink {
    my $self = shift;
    my ($file) = @_;

    if (exists $self->{link_task_for}{$file}) {
        my $task_ref = $self->{link_task_for}{$file};
        if ($task_ref->{action} eq 'remove') {
            debug(1, "UNLINK: $file (duplicates previous action)");
            return;
        }
        elsif ($task_ref->{action} eq 'create') {
            # Do need to create a link then remove it
            debug(1, "UNLINK: $file (reverts previous action)");
            $self->{link_task_for}{$file}->{action} = 'skip';
            delete $self->{link_task_for}{$file};
            return;
        }
        else {
            internal_error("bad task action: $task_ref->{action}");
        }
    }

    if (exists $self->{dir_task_for}{$file} and $self->{dir_task_for}{$file} eq 'create') {
        internal_error(
            "new unlink operation clashes with planned operation: %s dir %s",
            $self->{dir_task_for}{$file}->{action},
            $file
        );
    }

    # Remove the link
    debug(1, "UNLINK: $file");

    my $source = readlink $file or error("could not readlink $file ($!)");

    my $task = {
        action  => 'remove',
        type    => 'link',
        path    => $file,
        source  => $source,
    };
    push @{ $self->{tasks} }, $task;
    $self->{link_task_for}{$file} = $task;

    return;
}

#===== METHOD ===============================================================
# Name      : do_mkdir()
# Purpose   : wrap 'mkdir' operation
# Parameters: $dir => the directory to remove
# Returns   : n/a
# Throws    : fatal exception if operation fails
# Comments  : outputs a message if 'verbose' option is set
#           : does not perform operation if 'simulate' option is set
# Comments  : cleans up operations that undo previous operations
#============================================================================
sub do_mkdir {
    my $self = shift;
    my ($dir) = @_;

    if (exists $self->{link_task_for}{$dir}) {
        my $task_ref = $self->{link_task_for}{$dir};

        if ($task_ref->{action} eq 'create') {
            internal_error(
                "new dir clashes with planned new link (%s => %s)",
                $task_ref->{path},
                $task_ref->{source},
            );
        }
        elsif ($task_ref->{action} eq 'remove') {
            # May need to remove a link before creating a directory so continue
        }
        else {
            internal_error("bad task action: $task_ref->{action}");
        }
    }

    if (exists $self->{dir_task_for}{$dir}) {
        my $task_ref = $self->{dir_task_for}{$dir};

        if ($task_ref->{action} eq 'create') {
            debug(1, "MKDIR: $dir (duplicates previous action)");
            return;
        }
        elsif ($task_ref->{action} eq 'remove') {
            debug(1, "MKDIR: $dir (reverts previous action)");
            $self->{dir_task_for}{$dir}->{action} = 'skip';
            delete $self->{dir_task_for}{$dir};
            return;
        }
        else {
            internal_error("bad task action: $task_ref->{action}");
        }
    }

    debug(1, "MKDIR: $dir");
    my $task = {
        action  => 'create',
        type    => 'dir',
        path    => $dir,
        source  => undef,
    };
    push @{ $self->{tasks} }, $task;
    $self->{dir_task_for}{$dir} = $task;

    return;
}

#===== METHOD ===============================================================
# Name      : do_rmdir()
# Purpose   : wrap 'rmdir' operation
# Parameters: $dir => the directory to remove
# Returns   : n/a
# Throws    : fatal exception if operation fails
# Comments  : outputs a message if 'verbose' option is set
#           : does not perform operation if 'simulate' option is set
#============================================================================
sub do_rmdir {
    my $self = shift;
    my ($dir) = @_;

    if (exists $self->{link_task_for}{$dir}) {
        my $task_ref = $self->{link_task_for}{$dir};
        internal_error(
            "rmdir clashes with planned operation: %s link %s => %s",
            $task_ref->{action},
            $task_ref->{path},
            $task_ref->{source}
        );
    }

    if (exists $self->{dir_task_for}{$dir}) {
        my $task_ref = $self->{link_task_for}{$dir};

        if ($task_ref->{action} eq 'remove') {
            debug(1, "RMDIR $dir (duplicates previous action)");
            return;
        }
        elsif ($task_ref->{action} eq 'create') {
            debug(1, "MKDIR $dir (reverts previous action)");
            $self->{link_task_for}{$dir}->{action} = 'skip';
            delete $self->{link_task_for}{$dir};
            return;
        }
        else {
            internal_error("bad task action: $task_ref->{action}");
        }
    }

    debug(1, "RMDIR $dir");
    my $task = {
        action  => 'remove',
        type    => 'dir',
        path    => $dir,
        source  => '',
    };
    push @{ $self->{tasks} }, $task;
    $self->{dir_task_for}{$dir} = $task;

    return;
}

#===== METHOD ===============================================================
# Name      : do_mv()
# Purpose   : wrap 'move' operation for later processing
# Parameters: $src => the file to move
#           : $dst => the path to move it to
# Returns   : n/a
# Throws    : error if this clashes with an existing planned operation
# Comments  : alters contents of package installation image in stow dir
#============================================================================
sub do_mv {
    my $self = shift;
    my ($src, $dst) = @_;

    if (exists $self->{link_task_for}{$src}) {
        # I don't *think* this should ever happen, but I'm not
        # 100% sure.
        my $task_ref = $self->{link_task_for}{$src};
        internal_error(
            "do_mv: pre-existing link task for $src; action: %s, source: %s",
            $task_ref->{action}, $task_ref->{source}
        );
    }
    elsif (exists $self->{dir_task_for}{$src}) {
        my $task_ref = $self->{dir_task_for}{$src};
        internal_error(
            "do_mv: pre-existing dir task for %s?! action: %s",
            $src, $task_ref->{action}
        );
    }

    # Remove the link
    debug(1, "MV: $src -> $dst");

    my $task = {
        action  => 'move',
        type    => 'file',
        path    => $src,
        dest    => $dst,
    };
    push @{ $self->{tasks} }, $task;

    # FIXME: do we need this for anything?
    #$self->{mv_task_for}{$file} = $task;

    return;
}


#############################################################################
#
# End of methods; subroutines follow.
# FIXME: Ideally these should be in a separate module.


#===== PRIVATE SUBROUTINE ===================================================
# Name      : internal_error()
# Purpose   : output internal error message in a consistent form and die
# Parameters: $message => error message to output
# Returns   : n/a
# Throws    : n/a
# Comments  : none
#============================================================================
sub internal_error {
    my ($format, @args) = @_;
    my $error = sprintf($format, @args);
    my $stacktrace = Carp::longmess();
    die <<EOF;

$ProgramName: INTERNAL ERROR: $error$stacktrace

This _is_ a bug. Please submit a bug report so we can fix it! :-)
See http://www.gnu.org/software/stow/ for how to do this.
EOF
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

#############################################################################
# Default global list of ignore regexps follows
# (automatically appended by the Makefile)

__DATA__
# Comments and blank lines are allowed.

RCS
.+,v

CVS
\.\#.+       # CVS conflict files / emacs lock files
\.cvsignore

\.svn
_darcs
\.hg

\.git
\.gitignore

.+~          # emacs backup files
\#.*\#       # emacs autosave files

^/README.*
^/LICENSE.*
^/COPYING
