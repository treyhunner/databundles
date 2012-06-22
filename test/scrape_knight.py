'''
Created on Jun 21, 2012

@author: eric
'''


def main():
    import urllib, urlparse, pprint, csv
  
    from bs4 import BeautifulSoup

    urlf = "http://newschallenge.tumblr.com/page/{}"
    
    writer = csv.writer(open('/tmp/knight.csv', 'wb'))
    
    for i in range(1,79):
        url = urlf.format(i)
        print url

        doc = urllib.urlretrieve(url)

        for link in BeautifulSoup(open(doc[0])).find_all('a'):
            if len(link.contents) > 0 and link.contents[0] == 'VIEW ENTRY':
                page_url = link.get('href')
                page = urllib.urlretrieve(link.get('href'))
                pagebs = BeautifulSoup(open(page[0]))
                title = pagebs.find("div", "single").find('h2')
                
                if not title or len(title.contents) == 0: 
                    continue;
                
                try:
                    print i, page_url,',',title.contents[0]
                
                    writer.writerow([page_url,title.contents[0]])
                except Exception as e:
                    print "error for", url, e 
                    
                
            

if __name__ == '__main__':
    main()