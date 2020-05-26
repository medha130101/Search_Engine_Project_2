# REMEMBER -
# 1. The pages table consists of all the web pages retrieved from the URL by user input
# 2. The webs consists of a lists of all the accesiible web URLs that are to be traversd and checked initiall only.
# 3. 'Links' is just a many to many table which shows that page01 points to all the corresponding pages retrieved.
# 4. The 'Links' table basically shows the relaton between the consecutive web pages being extracted
# as the pages retrieved from a URL get all added to the 'Pages' table and ultimately we are going to check from the
# 'links' table only that which poge was retrieved from which page
import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
#BeautifulSoup is used to parse the data from the web to store it in a database.
from bs4 import BeautifulSoup
#In this code we are going to read URLs , creating databases using the sqlite3 library
#and then parse them using BeautifulSoup library
# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
#Making a new file 'spider.sqlite3' to store the data
conn = sqlite3.connect('spidernew.sqlite')
cur = conn.cursor()
#Creating the table if it does not exists
cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')
#This table is for a Many-to-Many relattionship
cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# Check to see if we are already in progress...
# We limit ourselves to pick up only a random URL which
# satisfies all the given condition in the SQL query as listed below.
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()
if row is not None:
    #The statement prints when we are going in depth and looking for a the web pages at a level deeper relatively.
    #It just picks up a random page from the list of retrieved pages
    print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
else :
# If the entire code is fresh it will prompt us to enter the URL to begin the process with
    starturl = input('Enter web url or enter: ')
    if ( len(starturl) < 1 ) : starturl = 'http://www.dr-chuck.com/'
    if ( starturl.endswith('/') ) : starturl = starturl[:-1]
    web = starturl
    if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
        pos = starturl.rfind('/')
        web = starturl[:pos]

    if ( len(web) > 1 ) :
        cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', ( web, ) )
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( starturl, ) )
        conn.commit()

# Get the current webs
cur.execute('''SELECT url FROM Webs''')
webs = list()
for row in cur:
    webs.append(str(row[0]))
# 'webs' is how many legit places are we going to go beacuse
# we can not wander on the entire internet aimlessly
print(webs)

many = 0
while True:
    if ( many < 1 ) :
# It prompts the user to ask for the pages to be retrieved
        sval = input('How many pages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)
    many = many - 1

    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        row = cur.fetchone()
        # print row
        #'fromid' is the page we are linking from
        fromid = row[0]
        url = row[1]
    except:
        print('No unretrieved HTML pages found')
        many = 0
        break
# Here it prints the formid and the URL
    print(fromid, url, end=' ')

    # If we are retrieving this page, there should be no links from it
    cur.execute('DELETE from Links WHERE from_id=?', (fromid, ) )
    try:
        document = urlopen(url, context=ctx)
# Here we are not going to use the .decode() as we are using BeautifulSoup which is
# going to compensate for the UTF-8 part
        html = document.read()
        if document.getcode() != 200 :
            print("Error on page: ",document.getcode())
            cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )
#This is for the pages we are not going to mess with
        if 'text/html' != document.info().get_content_type() :
            print("Ignore non text/html page")
            cur.execute('DELETE FROM Pages WHERE url=?', ( url, ) )
# Here we continue with only the URLs and not any other information present in the form of images or videoes
            conn.commit()
            continue

        print('('+str(len(html))+')', end=' ')
# Now we come on to the parsing part
        soup = BeautifulSoup(html, "html.parser")
# What happens if any key input is given unintentionally from the Keyboard Interrupt
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except:
        print("Unable to retrieve or parse page")
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        conn.commit()
        continue
#At this line we have got the HTML URL and so we are going to insert it in
#Also here we have set the initial page rank to 1.0
    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ) )
    conn.commit()

    # Retrieve all of the anchor tags
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        if ( href is None ) : continue
        # Resolve relative references like href="/contact"
        up = urlparse(href)
        if ( len(up.scheme) < 1 ) :
            href = urljoin(url, href)
        ipos = href.find('#')
        if ( ipos > 1 ) : href = href[:ipos]
    #We do not bother the other file format in the page
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
        if ( href.endswith('/') ) : href = href[:-1]
        # print href
        if ( len(href) < 1 ) : continue

		# Check if the URL is in any of the webs
        found = False
# webs were all the URLs that are willing to stay with us.
        for web in webs:
                if ( href.startswith(web) ) :
                    found = True
                    break
# We are going to skip the links that left the site
        if not found : continue
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) )
        count = count + 1
        conn.commit()
        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve id')
            continue
            # print fromid, toid
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )


    print(count)

cur.close()
