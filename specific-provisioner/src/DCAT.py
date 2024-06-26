import json
from io import BytesIO
from requests.auth import HTTPBasicAuth
import requests
import re
from lxml import etree
from io import StringIO
import logging

from typing import Optional

from rdflib import Graph, Literal, Namespace, RDF, URIRef, BNode
from rdflib.namespace import DCAT, DCTERMS, RDF, Namespace, RDFS, XSD, OWL, SKOS
from rdflib.plugins.sparql import prepareQuery

logger = logging.getLogger(__name__)

class DCATCatalog:
    def __init__(self, catalog_uri="http://data.example.com/catalog/main-catalog"):
        self.g = Graph()
        self.setup_namespaces()
        self.catalog = URIRef(catalog_uri)
        self.g.add((self.catalog, RDF.type, DCAT.Catalog))
        self.g.add((self.catalog, DCTERMS.title, Literal("Main Data Catalog")))
        self.g.add((self.catalog, DCTERMS.description, Literal("This is the main data catalog for our organization")))

        # Import the FIBO ontology --> load ext ontologies dynamically from config
        fibo_url = "https://spec.edmcouncil.org/fibo/ontology/master/2020Q1/prod.fibo-quickstart.ttl"

        # Download the ontology
        fibo_content = self.download_fibo_ontology(fibo_url)

        # Load the ontology into the RDF graph
        self.load_ontology_into_graph(fibo_content, format='turtle')



    def setup_namespaces(self):
        # Define namespaces
        self.CATALOG_NS = Namespace("http://data.example.com/catalog/")
        self.DATASET_NS = Namespace("http://data.example.com/dataset/")
        self.FIELD_NS = Namespace("http://data.example.com/fields/")

        # load domain ontologies from configuration and bind them dynamically ( out of scope for the prototype )
        self.FIBO_NS = Namespace("https://spec.edmcouncil.org/fibo/ontology/master/2020Q1/prod.fibo-quickstart.ttl#")
        self.EX_NS = Namespace("http://example.org/terms#")
        
        # Bind namespaces to prefixes for prettier output
        self.g.bind("dcat", DCAT)
        self.g.bind("dcterms", DCTERMS)
        self.g.bind("catalog", self.CATALOG_NS)
        self.g.bind("dataset", self.DATASET_NS)
        self.g.bind("field", self.FIELD_NS)
        self.g.bind("fibo", self.FIBO_NS)
        self.g.bind("ex", self.EX_NS)


    def download_fibo_ontology(self, url):
        """
        Download the FIBO ontology from a given URL.
        
        :param url: The URL of the FIBO ontology file
        :return: The content of the FIBO ontology file
        """
        try:
            logger.info(f"Attempting to download FIBO ontology from {url}")
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP errors
            logger.info("Successfully downloaded FIBO ontology")
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error downloading ontology: {e}")
            raise

    def load_ontology_into_graph(self, ontology_content, format='turtle'):
        """
        Load the FIBO ontology content into an RDFLib graph.
        
        :param ontology_content: The content of the FIBO ontology
        :param format: The format of the ontology content (default is 'turtle')
        :return: An RDFLib graph containing the FIBO ontology
        """
        
        self.g.parse(data=ontology_content, format=format)
        logger.info("Successfully loaded FIBO ontology into RDFLib graph")
        return self.g

    def is_business_term_in_fibo(self, term):
        """
        Check if a given business term is present in the FIBO namespace within the graph.
        
        :param term: The business term to check
        :return: True if the term exists in FIBO, False otherwise
        """

        # generalize with dynamic namespaces

        query = prepareQuery("""
            SELECT ?term ?label
            WHERE {
                ?term rdfs:label ?label .
                FILTER (str(?label) = "%s")
            }
        """ % term, initNs={"owl": OWL, "rdf": RDF, "fibo": self.FIBO_NS})

        results = self.g.query(query)
        return [f'{row.term}' for row in results]


    def list_fibo_business_terms(self,  limit: Optional[int] = None, filter: Optional[str] = None):
        """
        List all business terms present in the FIBO namespace within the graph.
        
        :return: A list of business terms (as strings)
        """

        # Prepare the filter condition
        filter_condition = ""
        if filter:
            filter_condition = f"FILTER (CONTAINS(LCASE(?label), LCASE('{filter}')))"

        # Prepare the query string with the optional filter condition
        query_string = """
            SELECT DISTINCT ?term ?label
            WHERE {
                ?term rdfs:label ?label .
                %s
            }
        """ % filter_condition

        # Append the LIMIT clause if limit is provided
        if limit:
            query_string += "LIMIT %d" % limit

        print(query_string)

        # Prepare the query
        query = prepareQuery(query_string, initNs={"owl": OWL, "rdf": RDF, "fibo": self.FIBO_NS})

        # Execute the query
        results = self.g.query(query)
        return [f'{row.term} --> {row.label}' for row in results]

    
    def dataset_exists(self, identifier):
        q = prepareQuery("""
        ASK {
            ?dataset a dcat:Dataset .
        }
        """, initNs={"dcat": DCAT, "dataset": self.DATASET_NS})
        return bool(self.g.query(q, initBindings={'dataset': URIRef(self.DATASET_NS[identifier])}))

    def delete_dataset(self, identifier):
        print(f'delete dataset {identifier}')
        dataset = URIRef(self.DATASET_NS[identifier])

        # Find all fields connected to the dataset
        fields = list(self.g.objects(dataset, self.FIELD_NS.hasField))
        for field in fields:
            # Remove all properties of the field node
            triples_to_remove = list(self.g.triples((field, None, None)))
            for triple in triples_to_remove:
                self.g.remove(triple)

        # Remove the hasField triples
        self.g.remove((dataset, self.FIELD_NS.hasField, None))

        # Find all business terms connected to the dataset
        business_terms = list(self.g.objects(dataset, RDFS.seeAlso))
        for bt in business_terms:
            # Remove only the connection to the business term, not the term itself
            self.g.remove((dataset, RDFS.seeAlso, bt))

        # Remove all other properties of the dataset node
        triples_to_remove = list(self.g.triples((dataset, None, None)))
        for triple in triples_to_remove:
            self.g.remove(triple)

        # Remove the connection from the catalog to the dataset
        self.g.remove((self.catalog, DCAT.dataset, dataset))


    def add_dataset(self, identifier, title, description, theme=None, keywords=None, issued=None, business_terms=None, fields=None):
        if self.dataset_exists(identifier):
            #witboost is always idempotent, so we cancel the previous version
            self.delete_dataset(identifier)


            
        
        dataset = URIRef(self.DATASET_NS[identifier])
        self.g.add((dataset, RDF.type, DCAT.Dataset))
        self.g.add((dataset, DCTERMS.title, Literal(title)))
        self.g.add((dataset, DCTERMS.description, Literal(description)))

        if theme:
            theme_node = BNode()
            self.g.add((theme_node, RDF.type, SKOS.Concept))
            self.g.add((theme_node, SKOS.prefLabel, Literal(theme)))
            self.g.add((dataset, DCAT.theme, theme_node))
        
        if keywords:
            for keyword in keywords:
                self.g.add((dataset, DCAT.keyword, Literal(keyword)))
        
        if issued:
            self.g.add((dataset, DCTERMS.issued, Literal(issued, datatype=XSD.date)))

        if business_terms:
            for term in business_terms:
                term_uri = URIRef(self.FIBO_NS[term])
                self.g.add((dataset, RDFS.seeAlso, term_uri))
                self.g.add((term_uri, RDF.type, OWL.Class))

        if fields:
            for field in fields:
                field_node = BNode()  # Create a blank node for each field
                self.g.add((field_node, RDF.type, self.FIELD_NS.Field))
                self.g.add((field_node, self.FIELD_NS.fieldName, Literal(field['name'])))
                self.g.add((field_node, self.FIELD_NS.fieldType, Literal(field['type'])))
                if 'description' in field:
                    self.g.add((field_node, self.FIELD_NS.fieldDescription, Literal(field['description'])))

                # Handle business terms for fields
                if 'business_terms' in field:
                    for bt in field['business_terms']:
                        print(bt)
                        matches = self.is_business_term_in_fibo(bt)
                        if len(matches) > 0:
                            
                            print(f'BT {bt} present in FIBO')
                            bt_uri = URIRef(self.FIBO_NS[matches[0]])    
                            print(bt_uri)
                            self.g.add((field_node, self.FIELD_NS.hasBusinessTerm, bt_uri))
                            self.g.add((bt_uri, RDF.type, OWL.Class))  # Ensure business term is also typed as an OWL Class


                self.g.add((dataset, self.FIELD_NS.hasField, field_node))
        
        self.g.add((self.catalog, DCAT.dataset, dataset))
        return True
    
    def add_foreign_key_relationship(self, from_dataset, to_dataset, description=None):
        if not self.dataset_exists(from_dataset) or not self.dataset_exists(to_dataset):
            raise ValueError("One or both datasets do not exist.")
        
        from_uri = URIRef(self.DATASET_NS[from_dataset])
        to_uri = URIRef(self.DATASET_NS[to_dataset])
        
        self.g.add((from_uri, self.EX_NS.hasForeignKeyTo, to_uri))
        
        if description:
            self.g.add((from_uri, RDFS.comment, Literal(description)))

        return True

    def serialize_catalog(self, format="json-ld", indent=2):
        json_ld = self.g.serialize(format="json-ld", indent=2)
        json_ld_dict = json.loads(json_ld)
        return json_ld_dict
