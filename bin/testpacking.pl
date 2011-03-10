#!/usr/bin/perl

use strict;
use warnings;

use MooseX::TimestampTZ;

use Git;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use VCS::Git::Torrent;
use VCS::Git::Torrent::CommitReel::Local;
use VCS::Git::Torrent::Reference;
use Set::Object qw(set);
use Scriptalicious;
use POSIX qw(ceil);

#print STDERR "new Git\n";
my $repo = Git->repository('.');
#print STDERR "new Torrent\n";
my $torrent = VCS::Git::Torrent->new(
	git => $repo,
);

my $num_blocks;
my $block_size;
my $slice_size = 0;

getopt(
	"blocks|b=i" => \$block_size,
	"numblocks|n=i" => \$num_blocks,
);

my %refs;
if ( @ARGV ) {
	%refs = map {
		my $name = `git rev-parse --symbolic-full-name $_`;
		chomp($name);
		my $sha1 = `git rev-parse $_`;
		chomp ($sha1);
		($name => $sha1)
	} @ARGV;
}
else {
	%refs = map { reverse m{^([0-9a-f]{40})\s+(\S+)$} } `git show-ref`;
}

#print STDERR "new Reference\n";
my $ref = VCS::Git::Torrent::Reference->new(
	torrent => $torrent,
	#tagged_object => 'a37da7d0996cf003e21687d8df9fa7685465d92f',
	refs => \%refs,
	tagger => 'Joe Foo Bob <foo@bar.baz>',
	tagdate => '2009-08-20 11:25:00+1200',
	comment => "foo\n",
);

#print STDERR "new CommitReel\n";
my $reel = VCS::Git::Torrent::CommitReel::Local->new(
	torrent => $torrent,
	end => $ref,
);


#print STDERR "connect reel and torrent\n";
$torrent->reels([$reel]);

print "Generating index...\n";
my $iter = $reel->index->reel_revlist_iter();

my $reel_size = $reel->index->size;

if ( $num_blocks ) {
	$block_size = ceil($reel_size / $num_blocks);
}
else {
	$block_size ||= 32;
	$block_size *= 1024;
}
print "Length is $reel_size, ".int(($reel_size+$block_size-1)/$block_size)." blocks of $block_size each\n";

my $inter_commit_size = 0;

my (@commits_in_range);
#my $seen_commits = set;
my (@last_seen);

my $slice_num = 0;
my $total_compressed;
my $commit_size = 0;

my $running_offset;
my $last_block = 0;
my $next_boundary = $block_size;

while ( my $rev = $iter->() ) {
	$commit_size += $rev->size;
	$running_offset += $rev->size;

	printf "%8i %8s %8i %s %s\n",
		$rev->offset,
		$rev->type,
		$rev->size,
		$rev->objectid,
		$rev->path ? $rev->path : '',
			if $VERBOSE > 0;

	if ( $rev->type eq 'commit' ) {
		print "--- at offset $running_offset, commit total size (w/objects): " . ($commit_size+$inter_commit_size) . "\n"
			if $VERBOSE > 0;

		if ( $next_boundary < $running_offset ) {
			my ($interesting, $uninteresting) = &find_interesting(\@commits_in_range, \@last_seen);#, $seen_commits);
			my $size = &do_pack($interesting, $uninteresting);
			printf "Slice #$slice_num (up to $next_boundary): $inter_commit_size => $size (%d%%)\n", ($size/$inter_commit_size)*100;
			$total_compressed += $size;
			$slice_num++;
			@last_seen = (@$interesting, @$uninteresting);
			@commits_in_range = ();
			$last_block++;
			$next_boundary = ceil($running_offset/$block_size)*$block_size;
			$inter_commit_size = $commit_size;
		}
		else {
			$inter_commit_size += $commit_size;
		}

		push @commits_in_range, $rev->objectid;
		$commit_size = 0;
	}
};

my ($interesting, $uninteresting) = &find_interesting(\@commits_in_range, \@last_seen);#, $seen_commits);
my $size = &do_pack($interesting, $uninteresting);
printf "Slice #$slice_num: $inter_commit_size => $size (%d%%)\n", ($size/$inter_commit_size)*100;
$total_compressed += $size;
printf "Overall: $running_offset => $total_compressed (%d%%)\n", ($total_compressed/$running_offset)*100;
system("git bundle create tmp$$.bundle @{[ keys %refs ]} 2>/dev/null");
$size = ( -s "tmp$$.bundle" );
unlink("tmp$$.bundle");
printf "vs Bundle: $running_offset => $size (%d%%)\n", ($size/$running_offset)*100;
printf("Overall inefficiency: %d%%\n", ( ($total_compressed - $size) / $size )* 100 );

sub do_pack {
	my ($interesting, $uninteresting) = @_;
	my $rev_list = shorten("@$interesting".(@$uninteresting?" --not @$uninteresting":""));
	print STDERR shorten("do_pack($rev_list)\n") if $VERBOSE == 0;
	my $cmd = "( git rev-list --objects $rev_list";
	if (@$uninteresting) {
		$cmd .= " | sed 's/ .*//'";
		$cmd .= "; git rev-list @$uninteresting | sed s/^/-/";
	}
	$cmd .= ") | ";
	#$cmd .= 'git pack-objects --stdout --incremental 2>/dev/null';
	$cmd = "for x in $rev_list; do echo \$x; done |";
	$cmd .= 'git pack-objects --stdout --thin 2>/dev/null';
#	$cmd .= ' > /dev/null';
	$cmd .= ' | wc -c';
	print STDERR $cmd . "\n" if $VERBOSE > 0;
	my $compressed_size = qx($cmd);
	0+$compressed_size;
}

sub find_interesting {
	my $commits_in_range = shift;
	my $last_seen = shift;
	#my $seen_commits = shift;

	my @interesting = $commits_in_range->[-1];
	my @uninteresting = @$last_seen;

	my $found = 0;
	my $extra = 0;
	#print STDERR "find_interesting: looking for ".@$commits_in_range." commits\n";
	my $round;
	my $trim_uninteresting = sub {
		my $size_before = @uninteresting;
		return if $size_before <= 1;
		my $cmd = "git rev-list --topo-order @uninteresting --not "
			.join(" ", map { "$_^" } @uninteresting);
		my $output = qx($cmd 2>&1);
		if ($?) {
			my @roots = ($output =~ m{ambiguous argument '([0-9a-f]+^)'}g);
			for my $root (@roots) {
				$cmd =~ s{\Q$root\E}{};
			}
			$output = qx($cmd);
			print $output if $?;
		}
		my @new_uninteresting = map { length $_ ? ($_) : () } split "\n", $output;
		if (@new_uninteresting) {
			#print STDERR "find_interesting: trimmed uninteresting list from $size_before to ".@uninteresting."\n";
			@uninteresting = @new_uninteresting;
		}
		else {
			#print STDERR "find_interesting: failed to get a new uninteresting list\n";
		}
	};
	while ($found < @$commits_in_range or $extra) {
		#print STDERR "find_interesting: round ".(++$round).", ".@interesting." interesting, ".@uninteresting." uninteresting\n";
		open REVLIST, "git rev-list --topo-order @interesting "
			.(@uninteresting?" --not @uninteresting":"")." |";
		my $expected = set(@$commits_in_range);
		$found = 0;
		$extra = 0;
		while (<REVLIST>) {
			my ($oid) = m{^([0-9a-f]{40})};
			if ($expected->includes($oid)) {
				$found++;
				$expected->remove($oid);
			}
			else {
				#print STDERR "find_interesting: found uninteresting commit $oid\n";
				push @uninteresting, $oid;
				$extra++;
				last if $extra > 10;
			}
		}
		close REVLIST;
		if ($extra) {
			$trim_uninteresting->();
		}
		elsif ($found < @$commits_in_range) {
			#print STDERR "find_interesting: found only $found commits (not ".@$commits_in_range.")\n";
			for (reverse @$commits_in_range) {
				if ($expected->includes($_)) {
					#print STDERR "find_interesting: found interesting commit $_\n";
					push @interesting, $_;
					last;
				}
			}
		}
	}
	$trim_uninteresting->();
	(\@interesting, \@uninteresting);
}

sub shorten {
	my $x = shift;
	$x =~ s{([0-9a-f]{12,})[a-f0-9]{28}}{$1}g;
	$x;
}
