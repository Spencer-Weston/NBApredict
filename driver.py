from HTMLparser import HTMLparser

class myHTMLparser(HTMLparser):
    def handle_starttag(self, tag, attrs):
        print "Encountered a start tag:", tag

parser = myHTMLparser
parser.feed("https://www.basketball-reference.com/leagues/NBA_2019.html")