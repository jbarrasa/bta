import codecs
from string import punctuation  
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from neo4j import GraphDatabase

stemmer = SnowballStemmer("english")
stopwords = stopwords.words('english')
non_words = list(punctuation)

def load_data(tx, country):
    cypher = " CREATE (sp:Speech { country: $country}) WITH sp" \
             " UNWIND $list as entry MERGE (st:Stem { id: entry.stem}) " \
             " MERGE (st)-[:USED_IN { freq: entry.count }]->(sp)" \
             " WITH st, entry.words as words  UNWIND words AS word " \
             " MERGE (w:Word { id: word })" \
             " MERGE (w)-[:HAS_STEM]->(st)" \
             " RETURN COUNT( DISTINCT st) AS stemCount ;"
    write_result = tx.run(cypher, list=process_file(country), country=country)
    print(write_result)



def process_file(country):
    file = codecs.open('data/' + country + '.txt' , encoding="utf-8")
    text = file.read()
    file.close()
    totalWC = len(text.split(" "))
    print("Approx word count: " + str(totalWC))

    cleantext = ''.join([c for c in text if c not in non_words]).lower()
    tokens = [tk for tk in word_tokenize(cleantext) if tk not in stopwords]

    print("\n\nClean word count (no stopwords): " + str(len(tokens)))

    tokensAndStems = []
    for tk in tokens:
        tokensAndStems.append([tk, stemmer.stem(tk)])

    # aggregate tokens on stems
    counts = dict()
    collects = dict()
    for pair in tokensAndStems:
        if pair[1] in counts:
            counts[pair[1]] += 1
            if pair[0] not in collects[pair[1]]: collects[pair[1]].append(pair[0])
        else:
            counts[pair[1]] = 1
            collects[pair[1]] = [pair[0]]

    # return as param list
    return [ { 'stem': key, 'count': value, 'words': collects[key] } for key, value in counts.items()]


uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "neo"))

with driver.session() as session:
    for country in ['sweden', 'germany','netherlands', 'uk', 'ireland','spain']:
        session.write_transaction(load_data, country)

driver.close()