"""
    The Zalando Crawler crawls the https://www.zalando.co.uk website and downloads images and related data content for both men and women.

    The two major functions of the Zalando class are building the database by first crawling all data and then assigning attributes in incremental steps.
"""

import requests
import json
from py2neo import Graph,authenticate,Node,Relationship
from bs4 import BeautifulSoup
import os,sys
from xml.etree.ElementTree import Element, SubElement, Comment, tostring

class Zalando:
    def __init__(self):

        self.logger = open("zalando_logger","a")
        self.logger.write("Checking for directory...\n")
        self.logger.write("Creating Zalando Object...\nInitializing Object variables...\n")
        self.set_directory()
        self.BASE_URL = "https://www.zalando.co.uk"
        self.initial_page = ['https://www.zalando.co.uk/womens-clothing','https://www.zalando.co.uk/mens-clothing']
        self.parameters = ['women','men']
        self.subcategories = [dict() for x in range(2)]
        authenticate("192.168.1.254:7474","neo4j","priyesh")
        self.zalando_graph = Graph()
        self.zalando_graph.schema.create_uniqueness_constraint('Zalando', 'article_number')
        self.count = 0
        self.xml_root = Element('Properties')
        self.properties = open("clothing_properties.xml","w")
"""
        self.logger.write("Zalando Object: " + str(self)+"\n")

        for i in range(len(self.initial_page)):
            self.c = 1
            page_html = requests.get(self.initial_page[i]).content
            page_soup = BeautifulSoup(page_html,"lxml")
            self.get_clothing_attributes(page_soup,i)
            self.build_zalando_database(page_soup,self.parameters[i])
            self.set_attributes(i)
        self.properties.write(tostring(self.xml_root)) """


    def set_directory(self):
        print "Setting up directory"

        if os.path.exists("Zalando"):
            self.logger.write("Zalando directory exists. Changing directory...\n")
            os.chdir("Zalando")

        else:
            self.logger.write("Zalando directory does not exist. Creating Zalando...\n")
            os.makedirs("Zalando")
            os.chdir("Zalando")

        self.logger.write("setting up required xml files...\n")
        #create xml files for attributes

    def set_attributes(self,i):
        count = 5
        for key in self.subcategories[i]:
            if count > 0 :
                product_type = self.subcategories[i][key].split("*")[0]
                next_page =   self.BASE_URL + self.subcategories[i][key].split("*")[1]
                count = count - 1
                while next_page is not None :
                    try:
                        attribute_soup = BeautifulSoup(requests.get(next_page).content,"lxml")
                    except Exception as e:
                        attribute_soup = BeautifulSoup(requests.get(next_page).content,"lxml")
                    catalog_articles_list = attribute_soup.find("ul","catalogArticlesList")
                    for li in catalog_articles_list.findAll("li"):
                        if str(type(li))=="<class 'bs4.element.Tag'>":
                            product_id = li['id']
                            try:
                                result = self.zalando_graph.cypher.execute("Match (n:Women) where n.article_number = '%s' set n.%s = '%s' " % (product_id,product_type,key))
                                print "result %s %s %s" %(str(result),product_id,key)
                            except Exception as e:
                                print e
                    if attribute_soup.find("a","catalogPagination_button catalogPagination_button-next") is not None:
                        next_page = self.BASE_URL + attribute_soup.find("a","catalogPagination_button catalogPagination_button-next")['href']
                    else:
                        next_page = None



    def get_product_details(self,product_url,gender):
        print self.count
        self.count = self.count + 1
        try:
            product_html = requests.get(product_url).content
        except Exception as e:
            product_html = requests.get(product_url).content
    	product_soup = BeautifulSoup(product_html,"lxml")
    	product_image = product_soup.find("img","articleMedia_imagePlaceholder")
    	article_span = product_soup.find("span","sku")
    	product_name = product_soup.find("h1","productName noBg")
    	brand = product_name.contents[1]

        product_info = dict()
    	product_info["article_number"] = article_span.string
    	product_info["brand"] = brand.string
        product_info["gender"] = gender
        product_info["attributes"] = ""

    	ul = article_span.parent.parent
    	self.logger.write("%s being explored" %(article_span.string)+"\n")
    	for li in ul.contents:
    		if li.string is not None and str(type(li))=="<class 'bs4.element.Tag'>":
    			var = li.get('class')[0]
    			if(len(li.string.split(":")) > 1 ):
    				product_info[li['class'][0]] = li.string.split(":")[1].encode("ascii","ignore")

    	n = Node(gender,"Zalando")
    	for key in product_info:
    		n.properties[key] = product_info[key]
    	self.zalando_graph.create(n)

    """
        Create a directory for each file being created
        while (image is not None):
    		url = image['src']
    		fileName = url.split('/')[-1]
    		url = url.replace("detail","large")
    		r = requests.get(url)
    		n.properties[fileName] = r.content
    		image = image.nextSibling.nextSibling
    """

    def get_clothing_attributes(self,page_soup,i):
        gender_dictionary = dict()
        gender_dictionary[0] = "/womens-clothing-"
        gender_dictionary[1] = "/mens-clothing-"
        child_element = SubElement(self.xml_root,self.parameters[i])
        self.logger.write("Getting clothing attributes")
        subcategories = page_soup.find("ul","subCat")
        self.subcategories[i] = dict()
        type_element = SubElement(child_element,"type")
        for li in subcategories.children :
    		if str(type(li))=="<class 'bs4.element.Tag'>" and li.contents[0].name == "a":
                    self.subcategories[i][li.contents[0]['href'].replace(gender_dictionary[i],"")[:-1]] = "type*"+li.contents[0]['href']
                    SubElement(type_element,"value").text = li.contents[0]['href'].replace(gender_dictionary[i],"")[:-1]
        color = page_soup.find("div","cFilter zFilter zal_color")
        color_element = SubElement(child_element,"color")
        for li in color.find_all("div")[1].find("div").find("ul").find_all("li"):
            if str(type(li))=="<class 'bs4.element.Tag'>" and li.contents[0].name == "a":
                self.subcategories[i][li.contents[0]['class'][0]] = "color*"+li.contents[0]['href']
                SubElement(color_element,"value").text = li.contents[0]['class'][0]
        pattern = page_soup.find("div","cFilter zFilter zal_pattern")
        pattern_element = SubElement(child_element,"pattern")
        for li in pattern.find_all("div")[1].find("div").find("ul").find_all("li"):
            if str(type(li))=="<class 'bs4.element.Tag'>" and li.contents[2].name == "a":
                self.subcategories[i][li.contents[0]['id']] = "pattern*"+li.contents[2]['href']
                SubElement(pattern_element,"value").text = li.contents[0]['id']
        if( i == 0 ):
            occasion = page_soup.find("div","cFilter zFilter zal_occasion")
            occasion_element = SubElement(child_element,"occasion")
            for li in occasion.find_all("div")[1].find("div").find("ul").find_all("li"):
                if str(type(li))=="<class 'bs4.element.Tag'>" and li.contents[2].name == "a":
                    self.subcategories[i][li.contents[0]['id']] = "occasion*"+li.contents[2]['href']
                    SubElement(occasion_element,"value").text = li.contents[0]['id']

        material = page_soup.find("div","cFilter zFilter zal_upper_material")
        material_element = SubElement(child_element,"material")
        for li in material.find_all("div")[1].find("div").find("ul").find_all("li"):
            if str(type(li))=="<class 'bs4.element.Tag'>" and li.contents[2].name == "a":
                self.subcategories[i][li.contents[0]['id']] = "material*"+li.contents[2]['href']
                SubElement(material_element,"value").text = li.contents[0]['id']
        self.logger.write(self.parameters[i] + " " + str(self.subcategories[i]) + "\n\n")


    def build_zalando_database(self,page_soup,gender):

        next_page = self.BASE_URL + page_soup.find("a","catalogPagination_button catalogPagination_button-next")['href']

        catalog_articles_list = page_soup.find("ul","catalogArticlesList")
        for li in catalog_articles_list.findAll("li"):
    		a = li.find("a","catalogArticlesList_productBox")
    		self.logger.write(self.BASE_URL+a["href"]+" being explored \n")
    		self.get_product_details(self.BASE_URL+a["href"],gender)
        self.c = self.c - 1
        while(self.c == 1 ):
            self.build_zalando_database(BeautifulSoup(requests.get(next_page).content,"lxml"),gender)

print "Creating Zalando Object"
Zalando()
