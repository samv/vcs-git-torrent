#!/usr/bin/perl -nl
use Git;
BEGIN{
    ($pid,$in,$out)=Git::command_bidi_pipe(
	"cat-file", "--batch-check");
    $show=sub{
	    $_=shift @l;
	    s{([a-f0-9]{7,40})}{($t=readline $in);chomp($t);$t}eg;
	    s{^([a-f0-9]{12})([a-f0-9]+)}{$1}g;
	    print;
    };
};
print {$out} $_ for m{([a-f0-9]{7,40})}g;
push @l, $_;
if(@l>40){
   $show->();
}
END{$show->() while @l}
