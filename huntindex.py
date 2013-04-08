# dev_appserver.py /dj2/web-google/ to run the development server
#   or with --clear_datastore before the folder name to do that.
# appcfg.py update /dj2/web-google/ to upload to the live web site
# python appcfg.py backends /dj2/web-google/ update maintenance
#   to update the backend
# appcfg.py vacuum_indexes /dj2/web-google/ to clear indexes on the live site
# Use the admin console to clear the datastore objects from the live site

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.datastore import entity_pb
from xml.dom.minidom import parseString
import urlparse
import cgi

def genlinks():
  r='<P style="text-align:right"><A HREF="/huntindex/">Home</A> '
  if users.is_current_user_admin():
    r+='<A HREF="/huntscripts/index">Admin</A> '
  r+="</P>"
  return r

def pageheader(subject="",short=0):
  r="""<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0//EN">
<html><head>
<link rel="stylesheet" type="text/css" href="/devjoe.css">
<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">
<title>MIT Mystery Hunt Puzzle Index"""
  if len(subject)>0:
    r+=": "+subject
  r+="""</title>
</head><body>"""+genlinks()
  if short==0:
    r+='<H1>MIT Mystery Hunt Puzzle Index'
    if len(subject)>0:
      r+=": "+subject
    r+='</H1>'
  elif len(subject)>0:
      r+="<H1>"+subject+"</H1>"
  return r

def pagefooter():
  return genlinks()+'</body></html>'

### CLASS DEFINITIONS ###

class Olist(db.Model):
  o=db.StringProperty(required=True)
  data=db.ListProperty(str)

class Keyword(db.Model):
  sortorder=db.StringProperty(required=True)
  fullname=db.TextProperty(required=True)
  exptext=db.TextProperty()
  puzzles=db.ListProperty(str, indexed=False)
  categories=db.ListProperty(str, indexed=False)
  puzzleexp=db.ListProperty(str, indexed=False)

class Puzzle(db.Model):
  sortorder=db.StringProperty(required=True)
  hunt=db.TextProperty() 
  num=db.TextProperty() 
  title=db.TextProperty(required=True) 
  exptext=db.TextProperty() 
  author=db.ListProperty(str, indexed=False)
  kwds=db.ListProperty(str, indexed=False)
  puzurl=db.LinkProperty(indexed=False)
  solurl=db.LinkProperty(indexed=False)

class Category(db.Model):
  sortorder=db.StringProperty(required=True)
  title=db.TextProperty(required=True)
  kwds=db.ListProperty(str,indexed=False)
  exptext=db.TextProperty(indexed=False)

class Author(db.Model):
  sortorder=db.StringProperty(required=True)
  title=db.TextProperty(required=True)
  puzzles=db.ListProperty(str,indexed=False)

### UTILITY FUNCTIONS FOR HANDLING HTML/XML DATA ###

wrap=cgi.escape

def cdata(x):
  #wrap a string that might contain tags in a CDATA tag for XML output
  if "<" in x or ">" in x or "&" in x:
    return "<![CDATA["+x+"]]>"
  else:
    return x

def gettext(node):
  #return all text in an xml node
  cnodes=node.childNodes
  c=""
  for cn in cnodes:
    if cn.nodeType == node.TEXT_NODE or cn.nodeType == node.CDATA_SECTION_NODE:
      c+=cn.data
  return c

def gettexts(nodes):
  #return the concatenation of all text in a set of xml nodes
  c=""
  for node in nodes:
    c+=gettext(node)
  return c

def getattr(node,attr):
  #return the named xml attribute
  if node.hasAttribute(attr):
    return node.getAttribute(attr)
  else:
    return ""

def showerr(handler,msg,node):
  #display an error about the XML file
  handler.response.out.write("<P>Error: "+msg+"</P>")
  handler.response.out.write("<PRE>"+wrap(node.toxml())+"</PRE>\n")

def getreqtag(handler,tag,node,nameofnode,nameoftag):
  #find a tag value under given xml node.
  #if there is no value or it is blank, show an error
  nodes=node.getElementsByTagName(tag)
  if len(nodes)==0:
    showerr(handler,nameofnode+" with no "+nameoftag,node)
    return ""
  else:
    rv=""
    for n in nodes:
      rv+=gettext(n)
    rv=rv.strip()
    if len(rv)==0:
      showerr(handler,nameofnode+" with empty "+nameoftag,node)
    return rv

def serialize(models):
  if models is None:
    return None
  elif isinstance(models, db.Model):
    #Just one instance
    return db.model_to_protobuf(models).Encode()
  else:
    #The only other type of argument should be a list.
    return [db.model_to_protobuf(x).Encode() for x in models]

def deserialize(data):
  if data is None:
    return None
  elif isinstance(data, str):
    #Just one instance
    return db.model_from_protobuf(entity_pb.EntityProto(data))
  else:
    #The only other type of argument should be a list.
    return [db.model_from_protobuf(entity_pb.EntityProto(x)) for x in data]

def clean_sortkey(k):
  #strip out all non-alphanumerics and make lower case
  k=k.lower()
  kk=""
  for j in k:
    if j in "abcdefghijklmnopqrstuvwxyz0123456789":
      kk+=j
  return kk

def check_url(u):
  x=urlparse.urlparse(u)
  if len(x[0])*len(x[1])==0:
    return 0
  else:
    return 1

### UTILITY FUNCTIONS FOR RETRIEVING DATA ###

def get_keyword(kw):
  data=memcache.get("keyword_"+kw)
  if data is not None: 
    return deserialize(data)
  else:
    kquery=db.Query(Keyword)
    kquery.filter("sortorder",kw)
    data=kquery.fetch(10)
    memcache.add("keyword_"+kw,serialize(data)) 
    return data

def get_puzzle(pz):
  data=memcache.get("puzzle_"+pz)
  if data is not None:
    return deserialize(data)
  else:
    pquery=db.Query(Puzzle)
    pquery.filter("sortorder",pz)
    data=pquery.fetch(10)
    memcache.add("puzzle_"+pz,serialize(data))
    return data

def get_category(ct):
  data=memcache.get("category_"+ct)
  if data is not None: 
    return deserialize(data)
  else:
    cquery=db.Query(Category)
    cquery.filter("sortorder",ct)
    data=cquery.fetch(10)
    memcache.add("category_"+ct,serialize(data)) 
    return data

def get_keywords(listonly=False):
  kws=memcache.get("keywords")
  if kws is None:
    oquery=db.Query(Olist)
    oquery.filter("o","keyword")
    kwobjlist=oquery.fetch(1)
    if len(kwobjlist)>0:
      kwobj=kwobjlist[0]
      kwlist=kwobj.data
    else:
      #keyword list does not exist at all. Create empty one.
      kwobj=Olist(o="keyword",data=[])
      kwobj.put()
      kwlist=[]
    memcache.add("keywords",serialize(kwobj))      
  else:
    kwobj=deserialize(kws)
    kwlist=kwobj.data
  if listonly:
    return kwobj
  kwmem=memcache.get_multi(kwlist,"keyword_")
  if len(kwlist)-len(kwmem.keys())>10:
    #get whole keyword set from database
    kquery=db.Query(Keyword)
    kquery.order("sortorder")
    data=kquery.fetch(100000)
    #this should match keywords item by item. Fix it if it is broken.
    kwck=[]
    for k in data:
      kwck.append(k.sortorder)
    if kwck!=kwlist:
      kwlist=kwck
      kwobj.data=kwlist
      kwobj.put()
    for kwi in xrange(len(kwlist)):
      kw=kwlist[kwi]
      if not kwmem.has_key(kw):
        memcache.add("keyword_"+kw,serialize([data[kwi]]))
    return data
  else:
    #look up missing keywords individually
    res=[]
    for kw in kwlist:
      if kwmem.has_key(kw):
        res.append(deserialize(kwmem[kw])[0])
      else:
        z=get_keyword(kw)
        if len(z)==0:
          #Handle failure by returning a temporary object that
          #never gets committed to the database
          z.append(Keyword(sortorder=kw,fullname="Error: keyword "+kw+
                           " does not exist",puzzles=[],categories=[],
                           puzzleexp=[]))
        res.append(z[0])
    return res

def get_puzzles(listonly=False):
  pzs=memcache.get("puzzles")
  if pzs is None:
    oquery=db.Query(Olist)
    oquery.filter("o","puzzle")
    pzobjlist=oquery.fetch(1)
    if len(pzobjlist)>0:
      pzobj=pzobjlist[0]
      pzlist=pzobj.data
    else:
      #puzzle list does not exist at all. Create empty one.
      pzobj=Olist(o="puzzle",data=[])
      pzobj.put()
      pzlist=[]
    memcache.add("puzzles",serialize(pzobj))      
  else:
    pzobj=deserialize(pzs)
    pzlist=pzobj.data
  if listonly:
    return pzobj
  pzmem={}
  for p in pzlist:
    q=memcache.get("puzzle_"+p)
    if q is not None:
      pzmem[p]=q
  if len(pzlist)-len(pzmem.keys())>10:
    #get whole puzzle set from database
    kquery=db.Query(Puzzle)
    kquery.order("sortorder")
    data=kquery.fetch(100000)
    #this should match puzzles item by item. Fix it if it is broken.
    pzck=[]
    for k in data:
      pzck.append(k.sortorder)
    if pzck!=pzlist:
      pzlist=pzck
      pzobj.data=pzlist
      pzobj.put()
    for pzi in xrange(len(pzlist)):
      pz=pzlist[pzi]
      if not pzmem.has_key(pz):
        memcache.add("puzzle_"+pz,serialize([data[pzi]]))
    return data
  else:
    #look up missing keywords individually
    res=[]
    for pz in pzlist:
      if pzmem.has_key(pz):
        res.append(deserialize(pzmem[pz])[0])
      else:
        z=get_puzzle(pz)
        if len(z)==0:
          #Handle failure by returning a temporary object that
          #never gets committed to the database
          z.append(Puzzle(sortorder=kw,hunt="",num="",
                          title="Error: puzzle "+kw+
                           " does not exist",exptext="",author="",kwds=[]
                           ))
        res.append(z[0])
    return res

def get_categories(listonly=False):
  cts=memcache.get("categories")
  if cts is None:
    oquery=db.Query(Olist)
    oquery.filter("o","category")
    ctobjlist=oquery.fetch(1)
    if len(ctobjlist)>0:
      ctobj=ctobjlist[0]
      ctlist=ctobj.data
    else:
      #category list does not exist at all. Create empty one.
      ctobj=Olist(o="category",data=[])
      ctobj.put()
      ctlist=[]
    memcache.add("categories",serialize(ctobj))      
  else:
    ctobj=deserialize(cts)
    ctlist=ctobj.data
  if listonly:
    return ctobj
  ctmem=memcache.get_multi(ctlist,"category_")
  if len(ctlist)-len(ctmem.keys())>10:
    #get whole category set from database
    cquery=db.Query(Category)
    cquery.order("sortorder")
    data=cquery.fetch(100000)
    #this should match categories item by item. Fix it if it is broken.
    ctck=[]
    for k in data:
      ctck.append(k.sortorder)
    if ctck!=ctlist:
      ctlist=ctck
      ctobj.data=ctlist
      ctobj.put()
    for cti in xrange(len(ctlist)):
      ct=ctlist[cti]
      if not ctmem.has_key(ct):
        memcache.add("category_"+ct,serialize([data[cti]]))
    return data
  else:
    #look up missing categories individually
    res=[]
    for ct in ctlist:
      if ctmem.has_key(ct):
        res.append(deserialize(ctmem[ct])[0])
      else:
        z=get_category(ct)
        if len(z)==0:
          #Handle failure by returning a temporary object that
          #never gets committed to the database
          z.append(Category(sortorder=ct,title="Error: category "+ct+
                           " does not exist",kwds=[]))
        res.append(z[0])
    return res

def get_keyword_selector():
  #retrieve the keyword select form field from memcache or rebuild it
  kform=memcache.get("keywordselector")
  if kform is not None:
    return kform
  kquery=get_keywords()
  res='<P>Keyword: <select name="kw1"/>'
  for kw in kquery:
    res+='<option value="'+kw.sortorder+'">'+wrap(kw.fullname)+'</option>'
  res+='</select></P>'
  #This is already a string, so no need to serialize it
  memcache.add("keywordselector",res)
  return res

def get_puzzle_selector():
  #retrieve the puzzle select form field from memcache or rebuild it
  #retrieve the keyword select form field from memcache or rebuild it
  pform=memcache.get("puzzleselector")
  if pform is not None:
    return pform
  pquery=get_puzzles()
  res='<P>Puzzle: <select name="pz1"/>'
  for pz in pquery:
    res+='<option value="'+pz.sortorder+'">'
    res+=wrap(pz.hunt+" "+pz.num+" "+pz.title)+'</option>'
  res+='</select></P>'
  #This is already a string, so no need to serialize it
  memcache.add("puzzleselector",res)
  return res

def get_category_selector():
  #retrieve the category select form field from memcache or rebuild it
  cform=memcache.get("categoryselector")
  if cform is not None:
    return cform
  cquery=get_categories()
  res='<P>Category: <select name="ct1"/>'
  for ct in cquery:
    res+='<option value="'+ct.sortorder+'">'+wrap(ct.title)+'</option>'
  res+='</select></P>'
  #This is already a string, so no need to serialize it
  memcache.add("categoryselector",res)
  return res

### MISCELLANEOUS FUNCTIONS ###

def getorder(things,thing):
  #perform binary search to determine where in sorted list things
  #object thing should be inserted
  a=0
  b=len(things)
  if len(things)==0 or thing<things[0]:
    return 0
  if thing>things[-1]:
    return b
  while b-a>1:
    c=int((a+b)/2)
    if things[c]<thing:
      a=c
    else:
      b=c
  return b

def get_index_state_as_string(index_state):
  return {db.Index.BUILDING:'BUILDING', db.Index.SERVING:'SERVING',
          db.Index.DELETING:'DELETING', db.Index.ERROR:'ERROR'} [index_state]

def get_sort_direction_as_string(sort_direction):
  return {db.Index.ASCENDING:'ASCENDING',
          db.Index.DESCENDING:'DESCENDING'}[sort_direction]

def dump_indexes():
  a=""
  for index, state in db.get_indexes():
    a+="<br>Kind: %s" % index.kind()
    a+="<br>State: %s" % get_index_state_as_string(state)
    a+="<br>Is ancestor: %s" % index.has_ancestor()
    for property_name, sort_direction in index.properties():
      a+= "<br>  %s:%s" % (property_name, get_sort_direction_as_string(sort_direction)) 
    return a

### WEB PAGE HANDLERS ###

class ShowKeyword(webapp.RequestHandler):
  def get(self,keyword):
    self.response.out.write(pageheader("Keyword Data"))
    kquery=get_keyword(keyword)
    for kw in kquery:
      self.response.out.write('<H2>'+kw.fullname+'</H2>')
      if kw.exptext!="":
        self.response.out.write('<P class="tight">'+kw.exptext+'</P>')
      self.response.out.write('<P><B>Puzzles</B></P>')
      #Get list of all puzzles with this keyword
      for pzi in range(len(kw.puzzles)):
        pz=get_puzzle(kw.puzzles[pzi])[0]
        self.response.out.write('<DIV class="puzlink">')
        self.response.out.write('<A HREF="/huntindex/puzzle/'+pz.sortorder+'" title="'+pz.num+'">')
        self.response.out.write(pz.hunt+" "+pz.title+'</A>')
        if len(kw.puzzleexp)>pzi:
          self.response.out.write(" "+kw.puzzleexp[pzi])
        self.response.out.write('</DIV>')
        if users.is_current_user_admin():
          self.response.out.write('''
          <form action="/huntscripts/keywordpuzzleunlink" method="post">
          <input type="hidden" name="kw1" value="'''+
          kw.sortorder+'''"/>
          <input type="hidden" name="pz1" value="'''+
          pz.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/keyword/'''+
          kw.sortorder+'''"/>
          <input class="linkdel" type="submit" value="Delete"></form>''')
        self.response.out.write('<P class="tight"><BR class="clear"></P>')
      self.response.out.write('<P><B>Categories</B></P>')
      #Get list of all categoriess with this keyword
      for cti in range(len(kw.categories)):
        ct=get_category(kw.categories[cti])[0]
        self.response.out.write('<DIV class="puzlink">')
        self.response.out.write('<A HREF="/huntindex/category/'+ct.sortorder+'">')
        self.response.out.write(ct.title+'</A>')
        self.response.out.write('</DIV>')
        if users.is_current_user_admin():
          self.response.out.write('''
          <form action="/huntscripts/keywordcategoryunlink" method="post">
          <input type="hidden" name="kw1" value="'''+
          kw.sortorder+'''"/>
          <input type="hidden" name="ct1" value="'''+
          ct.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/keyword/'''+
          kw.sortorder+'''"/>
          <input class="linkdel" type="submit" value="Delete"></form>''')
        self.response.out.write('<P class="tight"><BR class="clear"></P>')
      if users.is_current_user_admin():
        self.response.out.write('''<HR><P><B>Edit Keyword Data</B></P>
        <form action="/huntscripts/keywordedit" method="post">
          <P>Keyword: <input size="80" type="text" name="keyword" value="'''+
          wrap(kw.fullname,True)+'''"/></P>
          <input type="hidden" name="sortkey" value="'''+
          kw.sortorder+'''"/>
          <P>Explanation: <textarea name="exptext" rows="6" cols="80"
          >'''+wrap(kw.exptext,True)+'''</textarea></P>
          <input type="hidden" name="redir" value="/huntindex/keyword/'''+
          kw.sortorder+'''"/>
          <P><input type="submit" value="Submit"></P></form>''')
        self.response.out.write(
          '<P><A HREF="/huntscripts/kexpeditform/'+kw.sortorder+
                                '">Edit Link Explanatory Text</A></P>')
        self.response.out.write(
          '<HR><P><A HREF="/huntscripts/kplinkform/keyword/'+kw.sortorder+
                                '">Add a puzzle to this keyword</A></P>')
        self.response.out.write(
          '<P><A HREF="/huntscripts/kclinkform/keyword/'+kw.sortorder+
                                '">Add a category to this keyword</A></P>')
        self.response.out.write('''
          <HR><form action="/huntscripts/keyworddelete" method="post">
          <P><input type="submit" value="Delete Whole Keyword"></P>
          <P><input type="checkbox" name="confirm" value="Y">Confirm</P>
          <input type="hidden" name="sortkey" value="'''+
          kw.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/keyword/'''+
          kw.sortorder+'''"/>
          </form>''')
    self.response.out.write(pagefooter())

class ShowPuzzle(webapp.RequestHandler):
  def get(self,puzzle):
    self.response.out.write(pageheader("Puzzle Data"))
    pquery=get_puzzle(puzzle)
    for pz in pquery:
      if len(pz.author)>0:
        au=", ".join(pz.author)
      else:
        au=""
      self.response.out.write("<H2>"+pz.title+'</H2>')
      if len(pz.exptext)>0:
        self.response.out.write('<P class="tight">'+pz.exptext+'</P>')
      if len(pz.hunt)>0:
        self.response.out.write('<P class="tight">Hunt: '+pz.hunt+'</P>')
      if len(pz.num)>0:
        self.response.out.write('<P class="tight">Round/Puzzle# '+pz.num+'</P>')
      if len(au)>0:
        self.response.out.write('<P class="tight">Author: '+au+'</P>')
      if pz.puzurl is not None:
        self.response.out.write('<P class="tight"><A HREF="'+pz.puzurl+'">Puzzle page</A></P>')
      if pz.solurl is not None:
        self.response.out.write('<P class="tight"><A HREF="'+pz.solurl+'">Solution page</A></P>')
      self.response.out.write('<P class="tight"><BR><B>Keywords</B></P>')
      #Get list of all keywords for this puzzle
      for kwi in range(len(pz.kwds)):
        kw=get_keyword(pz.kwds[kwi])[0]
        self.response.out.write('<DIV class="puzlink">')
        self.response.out.write('<A HREF="/huntindex/keyword/'+kw.sortorder+'">'+kw.fullname+'</A>')
        self.response.out.write('</DIV>')
        if users.is_current_user_admin():
          self.response.out.write('''
          <form action="/huntscripts/keywordpuzzleunlink" method="post">
          <input type="hidden" name="kw1" value="'''+
          kw.sortorder+'''"/>
          <input type="hidden" name="pz1" value="'''+
          pz.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/puzzle/'''+
          pz.sortorder+'''"/>
          <input class="linkdel" type="submit" value="Delete"></form>''')
        self.response.out.write('<P class="tight"><BR class="clear"></P>')
      if users.is_current_user_admin():
        purl=pz.puzurl
        if purl is None:
          purl=""
        surl=pz.solurl
        if surl is None:
          surl=""
        self.response.out.write('''<HR><P><B>Edit Puzzle Data</B></P>
        <form action="/huntscripts/puzzleedit" method="post">
          <P>Title: <input size="80" type="text" name="title" value="'''+
          wrap(pz.title,True)+'''"/></P>
          <input type="hidden" name="sortkey" value="'''+
          pz.sortorder+'''"/>
          <P>Hunt: <input size="80" type="text" name="hunt" value="'''+
          wrap(pz.hunt,True)+'''"/></P>
          <P>Round/Puzzle# <input size="80" type="text" name="num" value="'''+
          wrap(pz.num,True)+'''"/></P>
          <P>Author: <input size="80" type="text" name="author" value="'''+
          wrap(au,True)+'''"/></P>
          <P>Puzzle URL: <input size="80" type="text" name="puzurl" value="'''+
          purl+'''"/></P>
          <P>Solution URL: <input size="80" type="text" name="solurl" value="'''+
          surl+'''"/></P>
          <P>Explanatory Text: <textarea name="exptext" rows="6" cols="80"
          >'''+wrap(pz.exptext,True)+'''</textarea></P>
          <input type="hidden" name="redir" value="/huntindex/puzzle/'''+
          pz.sortorder+'''"/>
          <P><input type="submit" value="Submit"></P></form>''')
        self.response.out.write(
          '<HR><P><A HREF="/huntscripts/kplinkform/puzzle/'+pz.sortorder+
                                '">Add a keyword to this puzzle</A></P>')
        self.response.out.write('''
          <HR><form action="/huntscripts/puzzledelete" method="post">
          <P><input type="submit" value="Delete Whole Puzzle"></P>
          <P><input type="checkbox" name="confirm" value="Y">Confirm</P>
          <input type="hidden" name="sortkey" value="'''+
          pz.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/puzzle/'''+
          pz.sortorder+'''"/>
          </form>''')
    self.response.out.write(pagefooter())

class ShowCategory(webapp.RequestHandler):
  def get(self,category):
    self.response.out.write(pageheader("Category Data"))
    #Get the category object for this category
    cquery=get_category(category)
    for ct in cquery:
      self.response.out.write('<H2>'+ct.title+'</H2>')
      if ct.exptext!="":
        self.response.out.write('<P class="tight">'+ct.exptext+'</P>')
      #Get list of all keywords in this category
      for kwi in range(len(ct.kwds)):
        kw=get_keyword(ct.kwds[kwi])[0]
        self.response.out.write('<DIV class="puzlink">')
        self.response.out.write('<A HREF="/huntindex/keyword/'+
                                kw.sortorder+'">')
        self.response.out.write(kw.fullname+'</A>')
        self.response.out.write('</DIV>')
        if users.is_current_user_admin():
          self.response.out.write('''
          <form action="/huntscripts/keywordcategoryunlink" method="post">
          <input type="hidden" name="kw1" value="'''+
          kw.sortorder+'''"/>
          <input type="hidden" name="ct1" value="'''+
          ct.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/category/'''+
          ct.sortorder+'''"/>
          <input class="linkdel" type="submit" value="Delete"></form>''')
        self.response.out.write('<P class="tight"><BR class="clear"></P>')
      if users.is_current_user_admin():
        self.response.out.write('''<HR><P><B>Edit Category Data</B></P>
        <form action="/huntscripts/categoryedit" method="post">
          <P>Category: <input size="80" type="text" name="category" value="'''+
          wrap(ct.title,True)+'''"/></P>
          <input type="hidden" name="sortkey" value="'''+
          ct.sortorder+'''"/>
          <P>Explanation: <textarea name="exptext" rows="6" cols="80"
          >'''+wrap(ct.exptext,True)+'''</textarea></P>
          <input type="hidden" name="redir" value="/huntindex/category/'''+
          ct.sortorder+'''"/>
          <P><input type="submit" value="Submit"></P></form>''')
        self.response.out.write(
          '<P><A HREF="/huntscripts/kclinkform/category/'+ct.sortorder+
                                '">Add a keyword to this category</A></P>')
        self.response.out.write('''
          <HR><form action="/huntscripts/categorydelete" method="post">
          <P><input type="submit" value="Delete Whole Category"></P>
          <P><input type="checkbox" name="confirm" value="Y">Confirm</P>
          <input type="hidden" name="sortkey" value="'''+
          ct.sortorder+'''"/>
          <input type="hidden" name="redir" value="/huntindex/category/'''+
          ct.sortorder+'''"/>
          </form>''')
    self.response.out.write(pagefooter())

class FullIndex(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("Full Index"))
    kquery=get_keywords()
    for kwk in kquery:
      kwlist=get_keyword(kwk.sortorder)
      if len(kwlist)!=1:
        self.response.out.write('<P>Error: Missing keyword'+kwk.sortorder+'</P>')
      else:
        kw=kwlist[0]
        self.response.out.write('<H2><A HREF="/huntindex/keyword/'+kw.sortorder+'">'+kw.fullname+'</A></H2>')
        if kw.exptext!="":
          self.response.out.write('<P class="tight">'+kw.exptext+'</P>')
        self.response.out.write('<P class="tighttop">')
        #Get list of all puzzles with this keyword
        for pzi in range(len(kw.puzzles)):
          pzlist=get_puzzle(kw.puzzles[pzi])
          if len(pzlist)!=1:
            self.response.out.write('Error: Missing puzzle:'+kw.puzzles[pzi]+'<BR>')
          else:
            pz=pzlist[0]
            self.response.out.write('<A HREF="/huntindex/puzzle/'+pz.sortorder+'" title="'+pz.num+'">')
            self.response.out.write(pz.hunt+" "+pz.title+'</A>')
            if len(kw.puzzleexp)>pzi:
              self.response.out.write(" "+kw.puzzleexp[pzi])
            self.response.out.write('<BR>')
        self.response.out.write('</P>')
    self.response.out.write(pagefooter())

class CategoryIndex(webapp.RequestHandler):
  def get(self,category):
    self.response.out.write(pageheader("Category Index"))
    #Get list of keywords in this category
    cquery=get_category(category)
    if len(cquery)==0:
      self.response.out.write(pagefooter())
      return
    ct=cquery[0]
    self.response.out.write('<H2>'+ct.title+'</H2>')
    if ct.exptext!="":
      self.response.out.write('<P class="tight">'+ct.exptext+'</P>')
    for kwi in ct.kwds:
      kwlist=get_keyword(kwi)
      if len(kwlist)!=1:
        self.response.out.write('<P>Error: Missing keyword'+kwk.sortorder+'</P>')
      else:
        kw=kwlist[0]
        self.response.out.write('<H2><A HREF="/huntindex/keyword/'+kw.sortorder+'">'+kw.fullname+'</A></H2>')
        if kw.exptext!="":
          self.response.out.write('<P class="tight">'+kw.exptext+'</P>')
        self.response.out.write('<P class="tighttop">')
        #Get list of all puzzles with this keyword
        for pzi in range(len(kw.puzzles)):
          pzlist=get_puzzle(kw.puzzles[pzi])
          if len(pzlist)!=1:
            self.response.out.write('Error: Missing puzzle:'+kw.puzzles[pzi]+'<BR>')
          else:
            pz=pzlist[0]
            self.response.out.write('<A HREF="/huntindex/puzzle/'+pz.sortorder+'" title="'+pz.num+'">')
            self.response.out.write(pz.hunt+" "+pz.title+'</A>')
            if len(kw.puzzleexp)>pzi:
              self.response.out.write(" "+kw.puzzleexp[pzi])
            self.response.out.write('<BR>')
        self.response.out.write('</P>')
    self.response.out.write(pagefooter())

class KeywordList(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("All Keywords"))
    kquery=get_keywords()
    for kwk in kquery:
      kwlist=get_keyword(kwk.sortorder)
      if len(kwlist)!=1:
        self.response.out.write('<P>Error: Missing keyword'+kwk.sortorder+'</P>')
      else:
        kw=kwlist[0]
        self.response.out.write('<P><A HREF="/huntindex/keyword/'+kw.sortorder+'">'+kw.fullname+'</A></P>')
    self.response.out.write(pagefooter())

class PuzzleList(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("All Puzzles"))
    #Get list of all puzzles, in sorted order
    pquery=get_puzzles()
    for pzk in pquery:
      pzlist=get_puzzle(pzk.sortorder)
      if len(pzlist)!=1:
        self.response.out.write('<P>Error: Missing puzzle'+pzk.sortorder+'</P>')
      else:
        pz=pzlist[0]
        self.response.out.write('<P><A HREF="/huntindex/puzzle/'+pz.sortorder+'" title="'+pz.num+'">'+pz.hunt+" "+pz.title+'</A></P>')
    self.response.out.write(pagefooter())

class CategoryList(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("All Categories"))
    #Get list of all categories, in sorted order
    cquery=get_categories()
    for cak in cquery:
      calist=get_category(cak.sortorder)
      if len(calist)!=1:
        self.response.out.write('<P>Error: Missing category'+cak.sortorder+'</P>')
      else:
        ca=calist[0]
        self.response.out.write('<P>'+ca.title+' <A HREF="/huntindex/category/'+ca.sortorder+'">(Keywords)</A> <A HREF="/huntindex/index/'+ca.sortorder+'">(Full category index)</A></P>')
    self.response.out.write(pagefooter())

class HomePage(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader())
    self.response.out.write('''<P>Yes, an index. So we can find the puzzles that have previously been done on a topic.</P>
<H2>What's in the Index?</H2>
<P>All the Hunts from 1994-1997 and 1999-2012 are indexed. 1999 has be re-indexed after the April 2013 archive update provided a lot of the missing information and solutions. Many thanks to Aaron Dinkin of Metaphysical Plant for helping get the 2011 puzzles added, and Seth Schoen of Codex for the 2012 puzzles, as well as all the people who have helped fill in gaps in the Mystery Hunt archive. The 2013 puzzles are being added.</P>
<H2>How do I access it?</H2>
<P>In any of these ways:</P>
<UL>
<LI><A HREF="/huntindex/index">See the complete index on one page</A> (very long)</LI>
<LI><A HREF="/huntindex/keywords">See the list of keywords</A></LI>
<LI><A HREF="/huntindex/puzzles">See the list of puzzles</A></LI>
<LI><A HREF="/huntindex/categories">See the list of categories</A></LI>
</UL>
<P>New features: There are now puzzle pages on this site which can contain additional information about specific puzzles, as well as separate puzzle and solution links and author credits. Other features will be added as time allows. Most Hunts before 2000 do not have any posted author credits for individual puzzles, so they have no author credits on this site.
</P>
<H2>What remains to be added?</H2>
<UL>
<LI>The 1984, 1986, 1987 MIT Mystery Hunts - solutions available</LI>
<LI>The 1988, 1990, and 1998 MIT Mystery Hunts - not in the archive.</LI>
<LI>Other old MIT Mystery Hunts - no solutions available currently</LI>
</UL>
<P>Future development: I am considering adding other non-Mystery Hunt puzzle hunts to this index. To be eligible, a puzzle hunt should:</P>
<UL>
<LI>Have freely accessible archives of puzzles online</LI>
<LI>Have a web site at a stable URL for a period of time</LI>
<LI>Have solutions available online somewhere, or I have solutions for them I can post</LI>
<LI>Not be an ongoing event people are expected to still be solving (i.e., solvers are not still asked to keep solutions secret)</LI>
</UL>
<P>Old MUMS, CISRA, SUMS (if they quit moving their web site around; one SUMS is missing), the Harvard hunt, DASH, BAPHL, the RIT CS hunts, Mark Halpin's Labor Day hunts, and the Googol Conglomerate game are candidates. PuzzleCrack fails on having a stable website; only 2007 and 2008 are up now, 2008 without solutions. Sekkrets, The Stone, and Panda Magazine are not eligible because access is still limited to what you have solved/opened via a logged in account/paid for. TimeHunt, Retrocogitator, and Puzzle Boat are not eligible because the puzzles are not online. </P>
''')
    kws=get_keywords(True)
    pzs=get_puzzles(True)
    cts=get_categories(True)
    self.response.out.write('<P>There are currently '+repr(len(kws.data))+' keywords and '+repr(len(pzs.data))+' puzzles and '+repr(len(cts.data))+' categories in the database.</P>')
    self.response.out.write(pagefooter())

class MainPage(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("Administration"))
    self.response.out.write('<P><A HREF="/huntindex/index">See whole index</A></P>')
    #Show the forms for adding a keyword or a puzzle
    self.response.out.write("""
        <HR><P><B>Add Keyword</B></P>
        <form action="/huntscripts/keywordsubmit" method="post">
          <P>Keyword: <input size="80" type="text" name="keyword"/></P>
          <P>Sortkey: <input size="80" type="text" name="sortkey"/>
            <small>This string is used to sort the keywords, and should
            normally be Keyword with all non-alphanumerics removed, but
            may differ to remove leading articles, sort by last name, 
            etc.</small></P>
          <P>Explanatory Text: <textarea name="exptext" cols="80" rows="6"
             ></textarea></P>
          <input type="hidden" name="redir" value="/huntscripts/index"/>
          <P><input type="submit" value="Submit"></P></form>""")
    self.response.out.write("""
        <HR><P><B>Add Puzzle</B></P>
        <form action="/huntscripts/puzzlesubmit" method="post">
          <P>Hunt Year: <input size="80" type="text" name="hunt"/></P>
          <P>Puzzle Number: <input size="80" type="text" name="num"/></P>
          <P>Puzzle Title: <input size="80" type="text" name="title"/></P>
          <P>Sortkey: <input size="80" type="text" name="sortkey"/>
            <small>This string is used to sort the puzzles. Recommended is
            abbreviated hunt name, four-digit year, round number (two digits
            if needed), puzzle number (in arbitrary format)</small></P>
          <P>Puzzle URL: <input size="80" type="text" name="puzurl"/></P>
          <P>Solution URL: <input size="80" type="text" name="solurl"/></P>
          <P>Author: <input size="80" type="text" name="author"/></P>
          <P>Explanatory Text: <textarea name="exptext" cols="80" rows="6"
             ></textarea></P>
          <input type="hidden" name="redir" value="/huntscripts/index"/>
          <P><input type="submit" value="Submit"></P></form>""")
    self.response.out.write("""
        <HR><P><B>Add Category</B></P>
        <form action="/huntscripts/categorysubmit" method="post">
          <P>Category: <input size="80" type="text" name="category"/></P>
          <P>Sortkey: <input size="80" type="text" name="sortkey"/>
            <small>This string is used to sort the categories, and should
            normally be Category with all non-alphanumerics removed, but
            may differ to remove leading articles, sort by last name, 
            etc.</small></P>
          <P>Explanatory Text: <textarea name="exptext" cols="80" rows="6"
             ></textarea></P>
          <input type="hidden" name="redir" value="/huntscripts/index"/>
          <P><input type="submit" value="Submit"></P></form>""")
    self.response.out.write('''<HR><P><B>Add Links</B></P>
        <A HREF="/huntscripts/kplinkform">Link puzzle to keyword</A></P>''')
    self.response.out.write('''<P><A HREF="/huntscripts/kclinkform">Link
         category to keyword</A></P>''')
    self.response.out.write('''
        <HR><P><B>Bulk Puzzle Data Upload</B></P>
        <form action="/huntscripts/uploaddata" enctype="multipart/form-data"
              method="post">
          <P>File: <input type="file" name="datafile" size="80"></P>
          <P>Clear all old data? 
          <input type="checkbox" name="clear" value="clear"></P>
        <input type="hidden" name="redir" value="/huntscripts/index"/>
        <P><input type="submit" value="Submit"></P></form>''')
    self.response.out.write('''<HR><P><B>Maintenance</B></P>
        <form action="/huntscripts/dedupe" method="post">
        <P><input type="submit" value="Deduplicate"></P></form>
        <form action="/huntscripts/fixlinks" method="post">
        <P><input type="submit" value="Fix Links"> (long, consider running from
        a <A HREF="http://maintenance.devjoe.appspot.com/huntscripts/index"
        >backend</A>)</P></form>''')
    self.response.out.write('''<P><A HREF="/huntscripts/downloaddata.xml">Bulk
        download puzzle database as XML</A></P>''')
    self.response.out.write('''<P><A HREF="/huntindexdoc/">Documentation</A>
        </P>''')
    self.response.out.write(pagefooter())

class KeywordPuzzleLinkForm(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("Link Keyword and Puzzle",short=1))
    kform=get_keyword_selector()
    pform=get_puzzle_selector()
    self.response.out.write('''
        <form action="/huntscripts/keywordpuzzlelink" method="post">''')
    self.response.out.write(kform+pform)
    self.response.out.write('''
        <P>Explanation: <input size="80" type="text" name="exptext"/> (optional)</P>
        <input type="hidden" name="redir" value="/huntscripts/kplinkform"/>
        <P><input type="submit" value="Submit"></P></form>''')
    stats = memcache.get_stats()
    self.response.out.write("End of page<br>")
    self.response.out.write("<b>Cache Hits:%s</b><br>" % stats['hits'])
    self.response.out.write("<b>Cache Misses:%s</b><br>" % stats['misses'])
    self.response.out.write("<b>Items in Cache:%s</b><br>" % stats['items'])
    self.response.out.write("<b>Cache Age (seconds):%s</b><br><br>" % stats['oldest_item_age'])
    self.response.out.write(pagefooter())

class KeywordCategoryLinkForm(webapp.RequestHandler):
  def get(self):
    self.response.out.write(pageheader("Link Keyword and Category",short=1))
    kform=get_keyword_selector()
    cform=get_category_selector()
    self.response.out.write('''
        <form action="/huntscripts/keywordcategorylink" method="post">''')
    self.response.out.write(kform+cform)
    self.response.out.write('''
        <input type="hidden" name="redir" value="/huntscripts/kclinkform"/>
        <P><input type="submit" value="Submit"></P></form>''')
    self.response.out.write(pagefooter())

class KeywordLinkForm(webapp.RequestHandler):
  def get(self,keyword):
    self.response.out.write(pageheader("Add a Puzzle to This Keyword",short=1))
    kw=get_keyword(keyword)[0]
    self.response.out.write('<P>'+kw.fullname+'</P>')
    pform=get_puzzle_selector()
    self.response.out.write('''
          <form action="/huntscripts/keywordpuzzlelink" method="post">
          <input type="hidden" name="kw1" value="'''+keyword+'"/>''')
    self.response.out.write(pform+'''
        <input type="hidden" name="redir" value="/huntindex/keyword/'''
                            +keyword+'''"/>
        <P><input type="submit" value="Submit"></P></form>''')
    self.response.out.write(pagefooter())

class PuzzleLinkForm(webapp.RequestHandler):
  def get(self,puzzle):
    self.response.out.write(pageheader("Add a Keyword to This Puzzle",short=1))
    pz=get_puzzle(puzzle)[0]
    self.response.out.write('<P>'+pz.hunt+' '+pz.num+' '+pz.title+'</P>')
    kform=get_keyword_selector()
    self.response.out.write('''
        <form action="/huntscripts/keywordpuzzlelink" method="post">''')
    self.response.out.write(kform+'''
          <input type="hidden" name="pz1" value="'''+puzzle+'''"/>
          <P>Explanation: <input size="80" type="text" name="exptext"/> (optional)</P>
          <input type="hidden" name="redir" value="/huntindex/puzzle/'''
                            +puzzle+'''"/>
          <P><input type="submit" value="Submit"></P></form>''')
    self.response.out.write(pagefooter())

class KeywordLinkCatForm(webapp.RequestHandler):
  def get(self,keyword):
    self.response.out.write(pageheader("Add a Category to This Keyword",short=1))
    kw=get_keyword(keyword)[0]
    self.response.out.write('<P>'+kw.fullname+'</P>')
    cform=get_category_selector()
    self.response.out.write('''
          <form action="/huntscripts/keywordcategorylink" method="post">
          <input type="hidden" name="kw1" value="'''+keyword+'"/>''')
    self.response.out.write(cform+'''
        <input type="hidden" name="redir" value="/huntindex/keyword/'''
                            +keyword+'''"/>
        <P><input type="submit" value="Submit"></P></form>''')
    self.response.out.write(pagefooter())

class CategoryLinkForm(webapp.RequestHandler):
  def get(self,category):
    self.response.out.write(pageheader("Add a Keyword to This Category",short=1))
    ct=get_category(category)[0]
    self.response.out.write('<P>'+ct.title+'</P>')
    kform=get_keyword_selector()
    self.response.out.write('''
          <form action="/huntscripts/keywordcategorylink" method="post">
          <input type="hidden" name="ct1" value="'''+category+'"/>''')
    self.response.out.write(kform+'''
        <input type="hidden" name="redir" value="/huntindex/category/'''
                            +category+'''"/>
        <P><input type="submit" value="Submit"></P></form>''')
    self.response.out.write(pagefooter())

class EditKeywordExpForm(webapp.RequestHandler):
  def get(self,keyword):
    self.response.out.write(pageheader("Edit Keyword Link Explanations",short=1))
    kquery=get_keyword(keyword)
    for kw in kquery:
      self.response.out.write('<H2>'+kw.fullname+'</H2>')
      if kw.exptext!="":
        self.response.out.write('<P class="tight">'+kw.exptext+'</P>')
      self.response.out.write('<P><B>Puzzles</B></P>')
      self.response.out.write('''<form action="/huntscripts/editkeywordexp" method="post">
        <input type="hidden" name="kw1" value="'''+keyword+'"/>')
      #Get list of all puzzles with this keyword
      for pzi in range(len(kw.puzzles)):
        pz=get_puzzle(kw.puzzles[pzi])[0]
        self.response.out.write('<DIV class="puzlink">')
        self.response.out.write('<A HREF="/huntindex/puzzle/'+pz.sortorder+'" title="'+pz.num+'">')
        self.response.out.write(pz.hunt+" "+pz.title+'</A>')
        self.response.out.write(' <input size="80" type="text" name="exptext'+repr(pzi)+'" value="')
        if len(kw.puzzleexp)>pzi:
          self.response.out.write(wrap(kw.puzzleexp[pzi],True))
        self.response.out.write('"/></DIV>')
        self.response.out.write('<P class="tight"><BR class="clear"></P>')
      self.response.out.write('''
      <input type="hidden" name="redir" value="/huntindex/keyword/'''+
      kw.sortorder+'''"/>
      <input type="submit" value="Submit"></form>''')
    self.response.out.write(pagefooter())

class Redirect(webapp.RequestHandler):
  def get(self):
    #get URL provided by user
    url=self.request.path
    if url=="/MIT_Mystery_Hunt_Puzzle.html":
      self.redirect("/huntindex/")
    elif url=="/huntindex/keywords.html":
      self.redirect("/huntindex/keywords")
    elif url=="/huntindex/Hunt_Index.html":
      self.redirect("/huntindex/index")
    elif len(url)>18 and url[:13]=="/huntindex/c_" and url[-5:]==".html":
      self.redirect("/huntindex/category/"+url[13:-5])
    elif len(url)>16 and url[:11]=="/huntindex/" and url[-5:]==".html":
      self.redirect("/huntindex/keyword/"+url[11:-5])
    else:
      #any unknown URL somehow arriving here goes back to the home page.
      self.redirect("/huntindex/")

### FORM RESPONSE HANDLERS ###

class AddKeyword(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    keyword=self.request.get('keyword')
    sortkey=self.request.get('sortkey')
    exptext=self.request.get('exptext')
    redir=self.request.get('redir')
    #clean keyword
    sortkey=clean_sortkey(sortkey)
    #Avoid duplicate keys
    g=get_keyword(sortkey)
    if len(g)>0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Error: That sortkey already exists! Keyword not made.</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    k=Keyword(sortorder=sortkey, fullname=keyword, exptext=exptext, puzzles=[], puzzleexp=[])
    k.put()
    memcache.delete("keyword_"+sortkey)
    #update keyword list
    o=get_keywords(True)
    opos=getorder(o.data,sortkey)
    o.data[opos:opos]=[sortkey]
    o.put()
    memcache.delete("keywords")
    memcache.delete("keywordselector")
    self.redirect("/huntindex/keyword/"+sortkey)

class EditKeyword(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    keyword=self.request.get('keyword')
    sortkey=self.request.get('sortkey')
    exptext=self.request.get('exptext')
    redir=self.request.get('redir')
    #Find existing key
    g=get_keyword(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Edit failed. Keyword does not exist!</P>')
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    g[0].fullname=keyword
    g[0].exptext=exptext
    g[0].put()
    memcache.delete("keyword_"+sortkey)
    self.redirect(redir)

class EditKeywordExp(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    sortkey=self.request.get('kw1')
    redir=self.request.get('redir')
    #Find existing key
    g=get_keyword(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Edit failed. Keyword does not exist!</P>')
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    exptext=[]
    isnonblank=0
    for i in range(len(g[0].puzzles)):
      e=self.request.get('exptext'+repr(i)).strip()
      exptext.append(e)
      if len(e)>0:
        isnonblank=1
    if isnonblank==0:
      exptext=[]
    g[0].puzzleexp=exptext
    g[0].put()
    memcache.delete("keyword_"+sortkey)
    self.redirect(redir)

class DelKeyword(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    sortkey=self.request.get('sortkey')
    confirm=self.request.get('confirm')
    redir=self.request.get('redir')
    #Find existing key
    g=get_keyword(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Delete failed. Keyword does not exist!</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    if confirm!="Y":
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Delete failed. You must check the confirm box.</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    #Process links first
    for pz1 in g[0].puzzles:
      pp=get_puzzle(pz1)
      if len(pp)==1:
        p=pp[0]
        if sortkey in p.kwds:
          pord=p.kwds.index(sortkey)
          del p.kwds[pord]
          p.put()
          memcache.delete("puzzle_"+pz1)
    for cat in g[0].categories:
      cc=get_category(cat)
      if len(cc)==1:
        c=cc[0]
        if sortkey in c.kwds:
          cord=c.kwds.index(sortkey)
          del c.kwds[cord]
          c.put()
          memcache.delete("category_"+cat)
    g[0].delete()
    memcache.delete("keyword_"+sortkey)
    #update keyword list
    o=get_keywords(True)
    try:
      opos=o.data.index(sortkey)
      del o.data[opos]
      o.put()
      memcache.delete("keywords")
      memcache.delete("keywordselector")
    except:
      #couldn't find keyword inlist of keywords?
      pass
    self.redirect("/huntscripts/index")

class AddPuzzle(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    hunt=self.request.get('hunt')
    num=self.request.get('num')
    title=self.request.get('title')
    sortkey=self.request.get('sortkey')
    sortkey=clean_sortkey(sortkey)
    redir=self.request.get('redir')
    exptext=self.request.get('exptext')
    au=self.request.get('author').strip()
    if len(au)==0:
      author=[]
    else:
      author=au.split(", ")
    puzurl=self.request.get('puzurl')
    if len(puzurl)==0:
      puzurl=None
    elif check_url(puzurl)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Invalid puzzle URL. "+puzurl+"</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    solurl=self.request.get('solurl')
    if len(solurl)==0:
      solurl=None
    elif check_url(solurl)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Invalid solution URL. "+solurl+"</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    #Avoid duplicate keys
    g=get_puzzle(sortkey)
    if len(g)>0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Error: That sortkey already exists! Puzzle not made.</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    k=Puzzle(hunt=hunt, num=num, title=title, sortorder=sortkey, kwds=[], puzurl=puzurl, solurl=solurl, exptext=exptext, author=author)
    k.put()
    memcache.delete("puzzle_"+sortkey)
    #update keyword list
    o=get_puzzles(True)
    opos=getorder(o.data,sortkey)
    o.data[opos:opos]=[sortkey]
    o.put()
    memcache.delete("puzzles")
    memcache.delete("puzzleselector")
    self.redirect("/huntindex/puzzle/"+sortkey)

class EditPuzzle(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    hunt=self.request.get('hunt')
    num=self.request.get('num')
    title=self.request.get('title')
    sortkey=self.request.get('sortkey')
    redir=self.request.get('redir')
    exptext=self.request.get('exptext')
    au=self.request.get('author').strip()
    if len(au)==0:
      author=[]
    else:
      author=au.split(", ")
    puzurl=self.request.get('puzurl')
    if len(puzurl)==0:
      puzurl=None
    elif check_url(puzurl)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Invalid puzzle URL. "+puzurl+"</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    solurl=self.request.get('solurl')
    if len(solurl)==0:
      solurl=None
    elif check_url(solurl)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Invalid solution URL. "+solurl+"</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    #Find existing key
    g=get_puzzle(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Edit failed. Puzzle does not exist!</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    g[0].hunt=hunt
    g[0].num=num
    g[0].title=title
    g[0].puzurl=puzurl
    g[0].solurl=solurl
    g[0].exptext=exptext
    g[0].author=author
    g[0].put()
    memcache.delete("puzzle_"+sortkey)
    self.redirect(redir)

class DelPuzzle(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    sortkey=self.request.get('sortkey')
    confirm=self.request.get('confirm')
    redir=self.request.get('redir')
    #Find existing key
    g=get_puzzle(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Delete failed. Puzzle does not exist!</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    if confirm!="Y":
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Delete failed. You must check the confirm box.</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    #Process links first
    for kw1 in g[0].kwds:
      kk=get_keyword(kw1)
      if len(kk)==1:
        k=kk[0]
        if sortkey in k.puzzles:
          kord=k.puzzles.index(sortkey)
          del k.puzzles[kord]
          if len(k.puzzleexp)>kord:
            del k.puzzleexp[kord]
          k.put()
          memcache.delete("keyword_"+kw1)
    g[0].delete()
    memcache.delete("puzzle_"+sortkey)
    #update keyword list
    o=get_puzzles(True)
    try:
      opos=o.data.index(sortkey)
      del o.data[opos]
      o.put()
      memcache.delete("puzzles")
      memcache.delete("puzzleselector")
    except:
      #couldn't find keyword inlist of keywords?
      pass
    self.redirect("/huntscripts/index")

class AddCategory(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    category=self.request.get('category')
    sortkey=self.request.get('sortkey')
    exptext=self.request.get('exptext')
    redir=self.request.get('redir')
    #clean keyword
    sortkey=clean_sortkey(sortkey)
    #Avoid duplicate keys
    g=get_category(sortkey)
    if len(g)>0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Error: That sortkey already exists! Category not made.</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    k=Category(sortorder=sortkey, title=category, exptext=exptext, kwds=[])
    k.put()
    memcache.delete("category_"+sortkey)
    #update category list
    o=get_categories(True)
    opos=getorder(o.data,sortkey)
    o.data[opos:opos]=[sortkey]
    o.put()
    memcache.delete("categories")
    memcache.delete("categoryselector")
    self.redirect("/huntindex/category/"+sortkey)

class EditCategory(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    category=self.request.get('category')
    sortkey=self.request.get('sortkey')
    exptext=self.request.get('exptext')
    redir=self.request.get('redir')
    #Find existing key
    g=get_category(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Edit failed. Category does not exist!</P>')
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    g[0].title=category
    g[0].exptext=exptext
    g[0].put()
    memcache.delete("category_"+sortkey)
    self.redirect(redir)

class DelCategory(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    sortkey=self.request.get('sortkey')
    confirm=self.request.get('confirm')
    redir=self.request.get('redir')
    #Find existing key
    g=get_category(sortkey)
    if len(g)==0:
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Delete failed. Category does not exist!</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    if confirm!="Y":
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write("<P>Delete failed. You must check the confirm box.</P>")
      self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
      self.response.out.write(pagefooter())
      return
    #Process links first
    for kw1 in g[0].kwds:
      kw=get_keyword(kw1)
      if len(kw)==1:
        k=kw[0]
        if sortkey in k.categories:
          cord=k.categories.index(sortkey)
          del k.categories[cord]
          k.put()
          memcache.delete("keyword_"+kw1)
    g[0].delete()
    memcache.delete("category_"+sortkey)
    #update category list
    o=get_categories(True)
    try:
      opos=o.data.index(sortkey)
      del o.data[opos]
      o.put()
      memcache.delete("categories")
      memcache.delete("categoryselector")
    except:
      #couldn't find category in list of categories?
      pass
    self.redirect("/huntscripts/index")

class AddKeywordPuzzleLink(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    redir=self.request.get('redir')
    kw1=self.request.get('kw1')
    kwlist=get_keyword(kw1)
    pz1=self.request.get('pz1')
    pzlist=get_puzzle(pz1)
    exptext=self.request.get('exptext')
    if len(kwlist)==1 and len(pzlist)==1:
      k=kwlist[0]
      p=pzlist[0]
      #Avoid duplicate links
      if pz1 in k.puzzles:
        self.response.out.write(pageheader("Error",short=1))
        self.response.out.write("<P>Error: That puzzle already associated with that keyword! Link not made.</P>")
        self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
        self.response.out.write(pagefooter())
        return
      kord=getorder(k.puzzles,pz1)
      pord=getorder(p.kwds,kw1)
      if len(exptext)>0 or len(k.puzzleexp)>0:
        if len(k.puzzleexp)<len(k.puzzles):
          k.puzzleexp+=[""]*(len(k.puzzles)-len(k.puzzleexp))
        k.puzzleexp[kord:kord]=[exptext]
      k.puzzles[kord:kord]=[str(pz1)]
      p.kwds[pord:pord]=[str(kw1)]
      k.put()
      p.put()
      memcache.delete("keyword_"+kw1)
      memcache.delete("puzzle_"+pz1)
      self.redirect(redir)
    else:
      #This should only happen if there is a bug or somebody is screwing around
      self.response.out.write("<html><body>Error: Invalid Link. ")
      self.response.out.write("kwlist="+repr(kwlist)+" ")
      self.response.out.write("pzlist="+repr(pzlist)+" ")
      self.response.out.write(pagefooter())

class DelKeywordPuzzleLink(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    redir=self.request.get('redir')
    kw1=self.request.get('kw1')
    kwlist=get_keyword(kw1)
    pz1=self.request.get('pz1')
    pzlist=get_puzzle(pz1)
    if len(kwlist)==1 and len(pzlist)==1:
      k=kwlist[0]
      p=pzlist[0]
      #Make sure link exists; allow fixing half-made links by deleting the
      #half that exists
      if pz1 not in k.puzzles and kw1 not in p.kwds:
        self.response.out.write(pageheader("Error",short=1))
        self.response.out.write("<P>Error: That puzzle not associated with that keyword! No link to delete.</P>")
        self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
        self.response.out.write(pagefooter())
        return
      if pz1 in k.puzzles:
        kpo=k.puzzles.index(pz1)
        del k.puzzles[kpo]
        if len(k.puzzleexp)>kpo:
          del k.puzzleexp[kpo]
      if kw1 in p.kwds:
        pko=p.kwds.index(kw1)
        del p.kwds[pko]
      k.put()
      p.put()
      memcache.delete("keyword_"+kw1)
      memcache.delete("puzzle_"+pz1)
      self.redirect(redir)
    else:
      #This should only happen if there is a bug or somebody is screwing around
      self.response.out.write("<html><body>Error: Invalid Link. ")
      self.response.out.write("kwlist="+repr(kwlist)+" ")
      self.response.out.write("pzlist="+repr(pzlist)+" ")
      self.response.out.write("kw1="+repr(kw1)+" ")
      self.response.out.write("pz1="+repr(pz1)+" ")
      self.response.out.write(pagefooter())

class AddKeywordCategoryLink(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    redir=self.request.get('redir')
    kw1=self.request.get('kw1')
    kwlist=get_keyword(kw1)
    ct1=self.request.get('ct1')
    ctlist=get_category(ct1)
    if len(kwlist)==1 and len(ctlist)==1:
      k=kwlist[0]
      c=ctlist[0]
      #Avoid duplicate links
      if ct1 in k.categories:
        self.response.out.write(pageheader("Error",short=1))
        self.response.out.write("<P>Error: That category already associated with that keyword! Link not made.</P>")
        self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
        self.response.out.write(pagefooter())
        return
      kord=getorder(k.categories,ct1)
      cord=getorder(c.kwds,kw1)
      k.categories[kord:kord]=[str(ct1)]
      c.kwds[cord:cord]=[str(kw1)]
      k.put()
      c.put()
      memcache.delete("keyword_"+kw1)
      memcache.delete("category_"+ct1)
      self.redirect(redir)
    else:
      #This should only happen if there is a bug or somebody is screwing around
      self.response.out.write("<html><body>Error: Invalid Link. ")
      self.response.out.write("kwlist="+repr(kwlist)+" ")
      self.response.out.write("ctlist="+repr(ctlist)+" ")
      self.response.out.write(pagefooter())

class DelKeywordCategoryLink(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    redir=self.request.get('redir')
    kw1=self.request.get('kw1')
    kwlist=get_keyword(kw1)
    ct1=self.request.get('ct1')
    ctlist=get_category(ct1)
    if len(kwlist)==1 and len(ctlist)==1:
      k=kwlist[0]
      c=ctlist[0]
      #Make sure link exists; allow fixing half-made links by deleting the
      #half that exists
      if ct1 not in k.categories and kw1 not in c.kwds:
        self.response.out.write(pageheader("Error",short=1))
        self.response.out.write("<P>Error: That category not associated with that keyword! No link to delete.</P>")
        self.response.out.write('<P><A HREF="'+redir+'">Try again?</A></P>')
        self.response.out.write(pagefooter())
        return
      if ct1 in k.categories:
        kpo=k.categories.index(ct1)
        del k.categories[kpo]
      if kw1 in c.kwds:
        pko=c.kwds.index(kw1)
        del c.kwds[pko]
      k.put()
      c.put()
      memcache.delete("keyword_"+kw1)
      memcache.delete("category_"+ct1)
      self.redirect(redir)
    else:
      #This should only happen if there is a bug or somebody is screwing around
      self.response.out.write("<html><body>Error: Invalid Link. ")
      self.response.out.write("kwlist="+repr(kwlist)+" ")
      self.response.out.write("ctlist="+repr(ctlist)+" ")
      self.response.out.write("kw1="+repr(kw1)+" ")
      self.response.out.write("ct1="+repr(ct1)+" ")
      self.response.out.write(pagefooter())

class UploadData(webapp.RequestHandler):
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    redir=self.request.get('redir')
    clear=self.request.get('clear')
    datafile=self.request.get('datafile')
    kwo=get_keywords(True)
    pzo=get_puzzles(True)
    cto=get_categories(True)
    self.response.out.write("<html><body>")
    if clear=="clear":
      #clear old database
      self.response.out.write("<P>Clearing all old data.</P>")
      kw=get_keywords()
      db.delete(kw)
      pw=get_puzzles()
      db.delete(pw)
      cw=get_categories()
      db.delete(cw)
      kwo.data=[]
      kwo.put()
      pzo.data=[]
      pzo.put()
      cto.data=[]
      cto.put()
      memcache.flush_all()
    x=parseString(datafile)
    nodes=x.documentElement.childNodes
    kws=[]
    pzs=[]
    cts=[]
    kwnames=[]
    pznames=[]
    ctnames=[]
    memcnames=[]
    for node in nodes:
      if node.nodeType==node.ELEMENT_NODE:
        tag=node.tagName
        if tag.lower()=="keyword":
          #create keyword object
          sortorder=getattr(node,"name")
          if len(sortorder)==0:
            showerr(self,"keyword with no sortkey",node)
            continue
          fullname=getreqtag(self,"fullname",node,"keyword","full name")
          if len(fullname)==0:
            showerr(self,"keyword with no name",node)
            continue
          #multiple exptext nodes, if present, get concatenated
          exptext=gettexts(node.getElementsByTagName("exptext"))
          #check for existing keyword. If it exists, merge.
          kquery=get_keyword(sortorder)
          if len(kquery)==0:
            puzzles=[]
            puzzleexp=[]
            realexp=0
            cats=[]
          else:
            kw=kquery[0]
            puzzles=kw.puzzles
            puzzleexp=kw.puzzleexp
            if len(puzzleexp)>0:
              realexp=1
            else:
              puzzleexp=[""]*len(puzzles)
              realexp=0
            cats=kw.categories
            if len(kw.exptext)>0 and exptext!=kw.exptext:
              exptext=kw.exptext+" "+exptext
          puzlinks=node.getElementsByTagName("puzlink")
          for puzlink in puzlinks:
            pz=getattr(puzlink,"name")
            if len(pz)==0:
              showerr(self,"Ignoring puzzle link with no name",puzlink)
              continue
            pzexp=gettext(puzlink)
            #insert new puzzle in order
            spz=str(pz)
            if spz in puzzles:
              continue
            go=getorder(puzzles,spz)
            puzzles[go:go]=[spz]
            puzzleexp[go:go]=[str(pzexp)]
            if len(pzexp)>0:
              realexp=1
          if realexp==0:
            puzzleexp=[]
          catlinks=node.getElementsByTagName("catlink")
          for catlink in catlinks:
            ct=getattr(catlink,"name")
            if len(ct)==0:
              showerr(self,"Ignoring category link with no name",catlink)
              continue
            #insert new category in order
            sct=str(ct)
            if sct in cats:
              continue
            go=getorder(cats,sct)
            cats[go:go]=[sct]
          if len(kquery)==0:
            kws.append(Keyword(sortorder=sortorder, fullname=fullname, exptext=exptext, puzzles=puzzles, categories=cats, puzzleexp=puzzleexp))
            kwnames.append(sortorder)
            memcnames.append("keyword_"+sortorder)
          else:
            kw.exptext=exptext
            #puzzles, puzzleexp, cats have been written directly into object
            kw.put()
            memcache.delete("keyword_"+sortorder)
        elif tag.lower()=="category":
          #create category object
          sortorder=getattr(node,"name")
          if len(sortorder)==0:
            showerr(self,"category with no sortkey",node)
            continue
          title=getreqtag(self,"title",node,"category","full name")
          if len(title)==0:
            showerr(self,"category with no name",node)
            continue
          #multiple exptext nodes, if present, get concatenated
          exptext=gettexts(node.getElementsByTagName("exptext"))
          #check for existing category. If it exists, merge.
          cquery=get_category(sortorder)
          if len(cquery)==0:
            keywords=[]
          else:
            ct=cquery[0]
            keywords=ct.kwds
            if len(ct.exptext)>0 and exptext!=ct.exptext:
              exptext=ct.exptext+" "+exptext
          kwlinks=node.getElementsByTagName("kwlink")
          for kwlink in kwlinks:
            kw=getattr(kwlink,"name")
            if len(kw)==0:
              showerr(self,"Ignoring keyword link with no name",kwlink)
              continue
            keywords.append(str(kw))
          if len(cquery)==0:
            cts.append(Category(sortorder=sortorder, title=title, kwds=keywords, exptext=exptext))
            ctnames.append(sortorder)
            memcnames.append("category_"+sortorder)
          else:
            ct.put()
            memcache.delete("category_"+sortorder)
        elif tag.lower()=="puzzle":
          #create puzzle object
          sortorder=getattr(node,"name")
          if len(sortorder)==0:
            showerr(self,"puzzle with no sortkey",node)
            continue
          hunt=getreqtag(self,"hunt",node,"puzzle","hunt")
          if len(hunt)==0:
            continue
          num=getreqtag(self,"num",node,"puzzle","number")
          if len(num)==0:
            continue
          title=getreqtag(self,"title",node,"puzzle","title")
          if len(title)==0:
            continue
          puzurls=node.getElementsByTagName("puzurl")
          if len(puzurls)>0:
            puzurl=getattr(puzurls[0],"href")
          else:
            puzurl=""
          if len(puzurl)==0:
            puzurl=None
          solurls=node.getElementsByTagName("solurl")
          if len(solurls)>0:
            solurl=getattr(solurls[0],"href")
          else:
            solurl=""
          if len(solurl)==0:
            solurl=None
          #multiple exptext nodes, if present, get concatenated
          exptext=gettexts(node.getElementsByTagName("exptext"))
          #check for existing puzzle. If it exists, merge.
          pquery=get_puzzle(sortorder)
          if len(pquery)==0:
            keywords=[]
            author=[]
          else:
            pz=pquery[0]
            keywords=pz.kwds
            author=pz.author
            if pz.puzurl is None and puzurl is not None:
              pz.puzurl=puzurl
            if pz.solurl is None and solurl is not None:
              pz.solurl=solurl
          kwlinks=node.getElementsByTagName("kwlink")
          for kwlink in kwlinks:
            kw=getattr(kwlink,"name")
            if len(kw)==0:
              showerr(self,"Ignoring keyword link with empty name",kwlink)
              continue
            #insert new keyword in order
            skw=str(kw)
            if skw in keywords:
              continue
            go=getorder(keywords,skw)
            keywords[go:go]=[skw]
          authors=node.getElementsByTagName("author")
          for author1 in authors:
            #au=getattr(author1,"name")
            au=gettext(author1)
            if len(au)==0:
              showerr(self,"Ignoring author with empty name",author1)
              continue
            #sau=str(au.encode('ascii','replace'))
            if au not in author:
              #we don't sort authors
              author.append(au)
          if len(pquery)==0:
            pzs.append(Puzzle(sortorder=sortorder, hunt=hunt, num=num, title=title, puzurl=puzurl, solurl=solurl, exptext=exptext, kwds=keywords, author=author))
            pznames.append(sortorder)
            memcnames.append("puzzle_"+sortorder)
          else:
            pz.put()
            memcache.delete("puzzle_"+sortorder)
    if len(kws)>0:
      memcache.delete("keywords")
      memcache.delete("keywordselector")
      kwo.data+=kwnames
      kwo.data.sort()
      kwo.put()
    if len(pzs)>0:
      memcache.delete("puzzles")
      memcache.delete("puzzleselector")
      pzo.data+=pznames
      pzo.data.sort()
      pzo.put()
    if len(cts)>0:
      memcache.delete("categories")
      memcache.delete("categoryselector")
      cto.data+=ctnames
      cto.data.sort()
      cto.put()
    db.put(kws)
    db.put(pzs)
    db.put(cts)
    memcache.delete_multi(memcnames)
    self.response.out.write("<P>Stored "+repr(len(kws))+" keywords and ")
    self.response.out.write(repr(len(pzs))+" puzzles and "+repr(len(cts))+" categories.</P>\n")
    self.response.out.write('<P><A HREF="'+redir+'">Back to editing page.</A></P>')
    self.response.out.write(pagefooter())

class DownloadData(webapp.RequestHandler):
  #Unlike most handlers, this one spits out an XML file intended for saving
  #directly to disk. It's accessed as a regular link with an HTML GET.
  def get(self):
    kws=get_keywords()
    pzs=get_puzzles()
    cts=get_categories()
    self.response.headers['Context-Type'] = 'text/xml'
    self.response.out.write('<?xml version="1.0" encoding="utf-8" ?>\n<hunt>\n')
    for kw in kws:
      self.response.out.write(' <keyword name="'+kw.sortorder+'">\n')
      self.response.out.write('  <fullname>'+cdata(kw.fullname)+'</fullname>\n')
      self.response.out.write('  <exptext>'+cdata(kw.exptext)+'</exptext>\n')
      for pzi in xrange(len(kw.puzzles)):
        pz=kw.puzzles[pzi]
        if len(kw.puzzleexp)>pzi:
          pzexp=kw.puzzleexp[pzi]
        else:
          pzexp=""
        self.response.out.write('  <puzlink name="'+pz+'">'+cdata(pzexp)+'</puzlink>\n')
      for ct in kw.categories:
        self.response.out.write('  <catlink name="'+ct+'"/>\n')
      self.response.out.write(' </keyword>\n')
    for pz in pzs:
      self.response.out.write(' <puzzle name="'+pz.sortorder+'">\n')
      self.response.out.write('  <hunt>'+cdata(pz.hunt)+'</hunt>\n')
      self.response.out.write('  <num>'+cdata(pz.num)+'</num>\n')
      self.response.out.write('  <title>'+cdata(pz.title)+'</title>\n')
      if pz.puzurl is not None:
        self.response.out.write('  <puzurl href="'+pz.puzurl+'"/>\n')
      if pz.solurl is not None:
        self.response.out.write('  <solurl href="'+pz.solurl+'"/>\n')
      self.response.out.write('  <exptext>'+cdata(pz.exptext)+'</exptext>\n')
      for au in pz.author:
        self.response.out.write('  <author>'+cdata(au)+'</author>\n')
      for kw in pz.kwds:
        self.response.out.write('  <kwlink name="'+kw+'"/>\n')
      self.response.out.write(' </puzzle>\n')
    for ct in cts:
      self.response.out.write(' <category name="'+ct.sortorder+'">\n')
      self.response.out.write('  <title>'+cdata(ct.title)+'</title>\n')
      self.response.out.write('  <exptext>'+cdata(ct.exptext)+'</exptext>\n')
      for kw in ct.kwds:
        self.response.out.write('  <kwlink name="'+kw+'"/>\n')
      self.response.out.write(' </category>\n')
    self.response.out.write("</hunt>\n")

class Dedupe(webapp.RequestHandler):
  #This needs to eventually be expanded to check for all kinds of
  #duplication, but for now I am just solving the problem I have, with
  #entries in the keyword list and the actual keyword objects. It's a
  #Post command, because it makes changes to the site, but it actually
  #doesn't take any input.
  #Later I might add checkboxes to select which kinds of deduping
  #I want to do.
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    self.response.out.write(pageheader("Deduplication Results",short=1))
    kws=get_keywords(True)
    for kw in kws.data:
      if kws.data.count(kw)>1:
        del kws.data[kws.data.index(kw)]
        self.response.out.write("<P>Deleting duplicate entry for "+kw+
                                " in keyword list.</P>")
      kwo=get_keyword(kw)
      if len(kwo)>1:
        #eliminate duplicate keywords
        #generally the first one is the one that gets everything, so use it
        #as the base. If any others have different, non-null properties then
        #add them in, and delete the rest.
        fullnames=[]
        exptexts=[]
        puzzles={}
        categories=[]
        for kwi in kwo:
          if len(kwi.fullname)>0 and kwi.fullname not in fullnames:
            fullnames.append(kwi.fullname)
          if len(kwi.exptext)>0 and kwi.exptext not in exptexts:
            exptexts.append(kwi.exptext)
          for pzi in range(len(kwi.puzzles)):
            if len(kwi.puzzleexp)<=pzi:
              exp=""
            else:
              exp=kwi.puzzleexp[pzi]
            if puzzles.has_key(kwi.puzzles[pzi]):
              if len(exp)>0:
                puzzles[kwi.puzzles[pzi]].append(exp)
            elif len(exp)>0:
              puzzles[kwi.puzzles[pzi]]=[exp]
            else:
              puzzles[kwi.puzzles[pzi]]=[]
          for ct in kwi.categories:
            if ct not in categories:
              categories.append(ct)
        if len(fullnames)>1:
          self.response.out.write("<P>Merging multiple fullnames for "
                              "keyword "+kw+"</P>")
        if len(exptexts)>1:
          self.response.out.write("<P>Merging multiple exptexts for "
                              "keyword "+kw+"</P>")
        kwo[0].fullname=", ".join(fullnames)
        kwo[0].exptext=", ".join(exptexts)
        categories.sort()
        kwo[0].categories=categories
        pzls=puzzles.keys()
        pzls.sort()
        empty=1
        etxt=[]
        for pzl in pzls:
          if len(puzzles[pzl])>1:
            self.response.out.write("<P>Merging multiple exptexts for "
                              "puzzle "+pzl+" under keyword "+kw+"</P>")
          if len(puzzles[pzl])>0:
            empty=0
          etxt.append(", ".join(puzzles[pzl]))
        if empty==1:
          etxt=[]
        kwo[0].puzzleexp=etxt
        kwo[0].puzzles=pzls
        for kwi in kwo[1:]:
          kwi.delete()
        kwo[0].put()
        memcache.delete("keyword_"+kw)
        self.response.out.write("<P>Merging multiple instances of"
                              " keyword "+kw+"</P>")
    kws.data.sort()
    kws.put()
    memcache.delete("keywords")
    memcache.delete("keywordselector")
    self.response.out.write("<P>"+repr(len(kws.data))+
                            " keywords remaining.</P>")
    self.response.out.write(pagefooter())

class FixLinks(webapp.RequestHandler):
  #Fix one-way links, by adding the link the other way.
  #Report all puzzles without keywords, keywords without puzzles,
  #Keywords without categories, and categories without keywords.
  def post(self):
    if not users.is_current_user_admin():
      self.response.out.write(pageheader("Error",short=1))
      self.response.out.write('<P>Error: Not admin. Action ignored.</P>')
      self.response.out.write(pagefooter())
      return
    self.response.out.write(pageheader("Link Fix Results",short=1))
    kws=get_keywords()
    pzs=get_puzzles()
    cts=get_categories()
    kwtoupdate=[]
    pztoupdate=[]
    cttoupdate=[]
    cachetoclear=[]
    for kw in kws:
      if len(kw.puzzles)==0:
        self.response.out.write('<P>Keyword <A HREF="/huntindex/keyword/'+
                                kw.sortorder+'">'+kw.fullname+
                                '</A> has no puzzle links.</P>')
      else:
        for pzlnk in kw.puzzles:
          pz=get_puzzle(pzlnk)
          if len(pz)==0:
            self.response.out.write('<P>Keyword <A HREF="/huntindex/keyword/'+
                                    kw.sortorder+'">'+kw.fullname+
                                    '</A> links to nonexistent puzzle'+
                                    pzlnk+'. Deleted link.</P>')
            del kw.puzzles[kw.puzzles.index(pzlnk)]
            if kw not in kwtoupdate:
              kwtoupdate.append(kw)
              cachetoclear.append("keyword_"+kw.sortorder)
          else:
            pz=pz[0]
            if kw.sortorder not in pz.kwds:
              self.response.out.write('<P>Fixed one-way link from keyword '+
                                    '<A HREF="/huntindex/keyword/'+
                                    kw.sortorder+'">'+kw.fullname+'</A> '+
                                    'to puzzle <A HREF="/huntindex/puzzle/'+
                                    pzlnk+'">'+pz.hunt+" "+pz.num+" "+
                                    pz.title+'</A>.</P>')
              pord=getorder(pz.kwds,kw.sortorder)
              pz.kwds[pord:pord]=[kw.sortorder]
              if pz not in pztoupdate:
                pztoupdate.append(pz)
                cachetoclear.append("puzzle_"+pzlnk)
      if len(kw.categories)==0:
        self.response.out.write('<P>Keyword <A HREF="/huntindex/keyword/'+
                                kw.sortorder+'">'+kw.fullname+
                                '</A> has no category links.</P>')
      else:
        for ctlnk in kw.categories:
          ct=get_category(ctlnk)
          if len(ct)==0:
            self.response.out.write('<P>Keyword <A HREF="/huntindex/keyword/'+
                                    kw.sortorder+'">'+kw.fullname+
                                    '</A> links to nonexistent category'+
                                    ctlnk+'.</P>')
            del kw.categories[kw.categories.index(ctlnk)]
            if kw not in kwtoupdate:
              kwtoupdate.append(kw)
              cachetoclear.append("keyword_"+kw.sortorder)
          else:
            ct=ct[0]
            if kw.sortorder not in ct.kwds:
              self.response.out.write('<P>Fixed one-way link from keyword '+
                                    '<A HREF="/huntindex/keyword/'+
                                    kw.sortorder+'">'+kw.fullname+'</A> '+
                                    'to category <A HREF="/huntindex/category/'+
                                    ctlnk+'">'+ct.title+'</A>.</P>')
              cord=getorder(ct.kwds,kw.sortorder)
              ct.kwds[cord:cord]=[kw.sortorder]
              if ct not in cttoupdate:
                cttoupdate.append(ct)
                cachetoclear.append("category_"+ctlnk)
    for pz in pzs:
      if len(pz.kwds)==0:
        self.response.out.write('<P>Puzzle <A HREF="/huntindex/puzzle/'+
                                pz.sortorder+'">'+pz.hunt+" "+pz.num+" "+
                                pz.title+'</A> has no keyword links.</P>')
      else:
        for kwlnk in pz.kwds:
          kw=get_keyword(kwlnk)
          if len(kw)==0:
            self.response.out.write('<P>Puzzle <A HREF="/huntindex/puzzle/'+
                                    pz.sortorder+'">'+pz.hunt+" "+pz.num+" "+
                                    pz.title+'</A> links to nonexistent '+
                                    'keyword '+kwlnk+'.</P>')
            del pz.kwds[pz.kwds.index(kwlnk)]
            if pz not in pztoupdate:
              pztoupdate.append(pz)
              cachetoclear.append("puzzle_"+pz.sortorder)
          else:
            kw=kw[0]
            if pz.sortorder not in kw.puzzles:
              self.response.out.write('<P>Fixed one-way link from puzzle '+
                                    '<A HREF="/huntindex/puzzle/'+pz.sortorder+
                                    '">'+pz.hunt+" "+pz.num+" "+pz.title+
                                    '</A> to keyword <A HREF="/huntindex/'+
                                    'keyword/'+kwlnk+'">'+kw.fullname+
                                    '</A>.</P>')
              kord=getorder(kw.puzzles,pz.sortorder)
              kw.puzzles[kord:kord]=[pz.sortorder]
              if kw not in kwtoupdate:
                kwtoupdate.append(kw)
                cachetoclear.append("keyword_"+kwlnk)
    for ct in cts:
      if len(ct.kwds)==0:
        self.response.out.write('<P>Category <A HREF="/huntindex/category/'+
                                ct.sortorder+'">'+
                                ct.title+'</A> has no keyword links.</P>')
      else:
        for kwlnk in ct.kwds:
          kw=get_keyword(kwlnk)
          if len(kw)==0:
            self.response.out.write('<P>Category <A HREF="/huntindex/category/'+
                                    ct.sortorder+'">'+
                                    ct.title+'</A> links to nonexistent '+
                                    'keyword '+kwlnk+'.</P>')
            del ct.kwds[ct.kwds.index(kwlnk)]
            if ct not in cttoupdate:
              cttoupdate.append(ct)
              cachetoclear.append("category_"+ct.sortorder)
          else:
            kw=kw[0]
            if ct.sortorder not in kw.categories:
              self.response.out.write('<P>Fixed one-way link from category '+
                                    '<A HREF="/huntindex/category/'+
                                    ct.sortorder+'">'+ct.title+
                                    '</A> to keyword <A HREF="/huntindex/'+
                                    'keyword/'+kwlnk+'">'+kw.fullname+
                                    '</A>.</P>')
              kord=getorder(kw.categories,ct.sortorder)
              kw.categories[kord:kord]=[ct.sortorder]
              if kw not in kwtoupdate:
                kwtoupdate.append(kw)
                cachetoclear.append("keyword_"+kwlnk)
    db.put(kwtoupdate)
    db.put(pztoupdate)
    db.put(cttoupdate)
    memcache.delete_multi(cachetoclear)
    self.response.out.write(pagefooter())

### LINK HANDLERS TO URLS ###

application=webapp.WSGIApplication([
    ('/huntscripts/index',MainPage),
    ('/huntscripts/keywordsubmit',AddKeyword),
    ('/huntscripts/keywordedit',EditKeyword),
    ('/huntscripts/keyworddelete',DelKeyword),
    ('/huntscripts/puzzlesubmit',AddPuzzle),
    ('/huntscripts/puzzleedit',EditPuzzle),
    ('/huntscripts/puzzledelete',DelPuzzle),
    ('/huntscripts/categorysubmit',AddCategory),
    ('/huntscripts/categoryedit',EditCategory),
    ('/huntscripts/categorydelete',DelCategory),
    ('/huntscripts/kplinkform',KeywordPuzzleLinkForm),
    (r'/huntscripts/kplinkform/puzzle/(.*)',PuzzleLinkForm),
    (r'/huntscripts/kplinkform/keyword/(.*)',KeywordLinkForm),
    ('/huntscripts/kclinkform',KeywordCategoryLinkForm),
    (r'/huntscripts/kclinkform/category/(.*)',CategoryLinkForm),
    (r'/huntscripts/kclinkform/keyword/(.*)',KeywordLinkCatForm),
    (r'/huntscripts/kexpeditform/(.*)',EditKeywordExpForm),
    ('/huntscripts/keywordpuzzlelink',AddKeywordPuzzleLink),
    ('/huntscripts/keywordpuzzleunlink',DelKeywordPuzzleLink),
    ('/huntscripts/keywordcategorylink',AddKeywordCategoryLink),
    ('/huntscripts/keywordcategoryunlink',DelKeywordCategoryLink),
    ('/huntscripts/editkeywordexp',EditKeywordExp),
    ('/huntscripts/uploaddata',UploadData),
    ('/huntscripts/downloaddata.xml',DownloadData),
    ('/huntscripts/dedupe',Dedupe),
    ('/huntscripts/fixlinks',FixLinks),
    (r'/huntindex/keyword/(.*)',ShowKeyword),
    (r'/huntindex/puzzle/(.*)',ShowPuzzle),
    (r'/huntindex/category/(.*)',ShowCategory),
    ('/huntindex/index',FullIndex),
    (r'/huntindex/index/(.*)',CategoryIndex),
    ('/huntindex/',HomePage),
    ('/huntindex/keywords',KeywordList),
    ('/huntindex/puzzles',PuzzleList),
    ('/huntindex/categories',CategoryList),
    ('/MIT_Mystery_Hunt_Puzzle.html',Redirect),
    (r'/huntindex/.*.html',Redirect)
], debug=True)

def main():
  run_wsgi_app(application)

if __name__ == '__main__':
  main()
