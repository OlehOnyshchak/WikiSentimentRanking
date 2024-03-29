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
        outfile.write("\n".join(data))
#         json.dump(data, outfile, indent=2, ensure_ascii=False)
        
def query_size(request, limit):
    site = pywikibot.Site()
    pages = list(site.search(request, namespaces=[0], total=limit))
    
    return len(pages)

def get_requests_path(request, out_dir):
    requests_base = Path(out_dir)
    requests_path = requests_base / request
    
    is_exist = requests_path.exists()
    if not is_exist:
        requests_path.mkdir(parents=True)
      
    return (requests_path, is_exist)

def query(request, out_dir='../requests', batch_size=100, limit=1000, is_category=False,
          preload_content=True, force_rewrite=True, debug_info=True):
    requests_path, existed = get_requests_path(request, out_dir)
    
    if existed:
        if not force_rewrite:
            if debug_info: print('Request has already been downloaded')
            return requests_path
        else:
            if debug_info: print('Cleaning old data')
            for x in requests_path.iterdir():
                x.unlink()
    
#     if debug_info: 
    print('Downloading...')
    site = pywikibot.Site()    
    if is_category:
        category = pywikibot.Category(site, request)
        pages = list(category.articles(namespaces=[0], # type of entities to query, 0 = page
                              recurse=True, # also query all subpages
                              total=limit,
                              content=preload_content)) # preloaod pages
    else:
        pages = list(site.search(request,
                                 total=limit,
                                 content=preload_content, # preloaod pages
                                 namespaces=[0])) # type of entities to query, 0 = page
    
    count = 0
    data = []
    for p in pages:
        count += 1
        data.append(json.dumps({
            "title": p.title(),
            "url": p.full_url(),
            "text": _clean(p.text),
        }))
        
        if count % batch_size == 0:
            _dump(requests_path / (str(count) + '.json'), data)
            data = []
            if debug_info: print('Dumped {} pages | {}'.format(count, request))
            
    if len(data):
        _dump(requests_path / (str(count) + '.json'), data)
        if debug_info: print('Dumped {} pages | {}'.format(count, request))
            
    return requests_path
