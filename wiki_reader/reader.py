from pathlib import Path
import pywikibot
from pywikibot import pagegenerators
import json
import mwparserfromhell as mwp

def _clean(wiki_text):
    wikicode = mwp.parse(wiki_text)
    return wikicode.strip_code()

def _dump(path, data):
    with open(path, 'w', encoding='utf8') as outfile:  
        json.dump(data, outfile, indent=2, ensure_ascii=False)

def query(request, batch_size=100, limit=1000):
    requests_base = Path('../requests')
    requests_path = requests_base / request
 
    if requests_path.exists():
        return

    requests_path.mkdir(parents=True)
    
    site = pywikibot.Site()
    category = pywikibot.Category(site, request)
    pages = category.articles(namespaces=[0], #type of entities to query, 0 = page
                              recurse=True, # also query all subpages
                              content = True) # preloaod pages
    
    count = 0
    pages_key = 'pages'
    data = { pages_key: [] }
    for p in pages:
        count += 1
        data[pages_key].append({
            'title': p.title(),
            'url': p.full_url(),
            'text': _clean(p.text),
        })
        
        if count % batch_size == 0:
            _dump(requests_path / (str(count) + '.json'), data)
            data = { pages_key: [] }
            print('Dumped {} pages'.format(count))
        
        if count == limit:
            break
            
    if len(data[pages_key]):
        _dump(requests_path / (str(count) + '.json'), data)