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
        
def query_size(request):
    site = pywikibot.Site()
    pages = list(site.search(request, namespaces=[0]))
    
    return len(pages)

def query(request, batch_size=100, limit=1000, is_category=False, debug_info=True):
    requests_base = Path('../requests')
    requests_path = requests_base / request
    
    if requests_path.exists():
        if debug_info: print('Request has already been downloaded')
        return requests_path

    requests_path.mkdir(parents=True, exist_ok=True)
    
    site = pywikibot.Site()
    
    if is_category:
        category = pywikibot.Category(site, request)
        pages = list(category.articles(namespaces=[0], # type of entities to query, 0 = page
                              recurse=True, # also query all subpages
                              total=limit,
                              content=True)) # preloaod pages
    else:
        pages = list(site.search(request,
                                 total=limit,
                                 content=True, # preloaod pages
                                 namespaces=[0])) # type of entities to query, 0 = page
    
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
            if debug_info: print('Dumped {} pages'.format(count))
            
    if len(data[pages_key]):
        _dump(requests_path / (str(count) + '.json'), data)
        if debug_info: print('Dumped {} pages'.format(count))
            
    return requests_path