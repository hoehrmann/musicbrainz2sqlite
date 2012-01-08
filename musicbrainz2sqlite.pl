#!perl -w
use strict;
use warnings;
use File::Spec;
use SQL::Translator;
use SQL::Translator::Parser::PostgreSQL;
use DBI;
use LWP::Simple;

die "Usage: $0 /path/to/mbdump musicbrainz.sqlite" unless @ARGV == 2;

my $dir = $ARGV[0];
my $sqlite_db = $ARGV[1];

my $tr = SQL::Translator->new;
$tr->parser("SQL::Translator::Parser::PostgreSQL");

my $sql = do {
  my $data = get('https://raw.github.com/metabrainz/musicbrainz-server/'
    . 'master/admin/sql/CreateTables.sql') or die
        "Unable to fetch CreateTables.sql";

  # SQL::Translator does not support these
  $data =~ s/^.*?BEGIN;//s;
  $data =~ s/COMMIT;.*?$//s;
  $data =~ s/\bUUID\b/TEXT/g;
  $data =~ s/DEFAULT -\d+//g;
  $data =~ s/\bid\s+SERIAL/id INTEGER PRIMARY KEY/g;

  $data;
};

my $sqlite = $tr->translate(to => 'SQLite', data => \$sql);

my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_db", "", "", {
  sqlite_allow_multiple_statements => 1
}) or die $DBI::errstr;

$dbh->do(q{
  PRAGMA page_size = 4096;
  PRAGMA journal_mode = OFF;
  PRAGMA synchronous = OFF;
}) or die $DBI::errstr;

$dbh->do($sqlite) or die $DBI::errstr;

# http://www.postgresql.org/docs/8.2/static/sql-copy.html

my %postgre_escapes = (
  N => undef,
  b => "\x08",
  f => "\x0c",
  n => "\x0a",
  r => "\x0d",
  t => "\x09",
  v => "\x0b",
);

foreach ($tr->schema->get_tables) {
  my $table_name = $_->name;
  open my $in, '<', File::Spec->catfile($dir, $table_name) or do {
    warn "Skipping $table_name (" . $! . ")\n";
    next;
  };

  my $values = join ",", (('?') x ($#{ $_->field_names } + 1));

  my $sth = $dbh->prepare("INSERT INTO " .
    $dbh->quote_identifier($table_name) . " VALUES($values)");

  $dbh->begin_work;
  my $count = 0;
  eval {
    while (<$in>) {
      chomp;
      my @fields = map {
        s|\\([bfnrtv])|$postgre_escapes{$1}|g;
        $_ eq "\\N" ? undef : $_;
      } split/\t/;
      $sth->execute(@fields);
      warn "Now at '$table_name' row #$count\n" if $count++ % 50_000 == 0;
    }
  };
  $dbh->commit;
}

