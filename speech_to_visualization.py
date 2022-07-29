import spacy
import pandas as pd
import sqlite3 as sql
import pandasql as psql
from matplotlib import pyplot as plt
import speech_recognition as sr
from scipy import spatial
from sent2vec.vectorizer import Vectorizer

def speech_to_text(): #error installing pyaudio. Solve: Use prerecorded audio file in .WAV format
    recognizer=sr.Recognizer()
    with sr.AudioFile("try.wav") as sound_source: #try.wav is an audio file containing the spoken statement
        audio = recognizer.record(sound_source)
        text = recognizer.recognize_google(audio)
        #print(text)
        return text
    '''
        print("What would you want to learn: ")
        audio=recognizer.listen(sound_source)

        try:
            text=recognizer.recognize_google(audio)
            #print(f"You said {text}")
        except:
            text='N/A'
            print("Could not understand what you said.")
    return text
    '''
nlp = spacy.load("en_core_web_sm")
#doc = nlp("show me year where sales are more than 45000")
#try show me year where sales are more than 60000 or show me sales after 2014
#doc=nlp("show me sales prior to 2013")

doc=nlp(f"{speech_to_text()}") #doc established as processing the output of the speech to text function

def tokenize():#returns sentence (from doc) as a list of tokens
    token_list=[]
    for token in doc:
        #print(token.text, token.pos_, token.dep_)
        token_list.append(token.text)
    return token_list

def remove_stopwords():#returns a list of tokens which are not stopwords.
    non_stop=[]
    for token in doc:
        if (token.is_stop==False):
            non_stop.append(token)
    #print(non_stop)
    return non_stop

#remove_stopwords()

def lemmat(): #lemmatize non-stop word list
    lemma_list=[]
    non_stop=remove_stopwords()
    for token in doc:
        if token in non_stop:
            lemma_list.append(token.lemma_)

    return lemma_list #['sale', '2014', '.']

lemma_nl=lemmat() #lemmatized list of non-stopword tokens

def named_entity_identifier(): #returns a list of all entities present in the doc(or sentence)
    #Note: NER WILL need to be trained separately to effectively work with datasets
    return list(doc.ents)
    '''
    years=[]
    for ent in doc.ents:
        print(ent.text, ent.label_, spacy.explain(ent.label_))
        if ent.label_.lower()=='date':
            years.append(ent.text)
    print(years)
    return years
    #ent.label_ is a string.
'''
#print(named_entity_identifier())


def load_db(): #Load table as pandas dataframe
    conne = sql.connect("your_database.db", check_same_thread=False)
    query=pd.read_sql_query('''SELECT * FROM SalesD''', conne)
    df_sq=pd.DataFrame(query, columns = ['Year', 'Sales']) #assuming used my generated table.. else put column names
    return df_sq
df_sq=load_db() #variable holding the table as a pd dataframe


def sel_col(): #determine the column that the SELECT command will use. Eg: SELECT 'Sales or year or *'
    cols=list(df_sq.columns) #list of columns in the table
    doc2=nlp(str(cols)) #creating an nlp model for the column names for NLP processing
    lemma_cols=[] #list of columns with their lemmatized name
    for token in doc2:
        if token.is_alpha: #punctuation gets added if this is not checked.... column name cannot be special character
            lemma_cols.append(token.lemma_)

    col_lcol={} #dictionary to map lemmatized columns to actual column names
    for i in range(len(lemma_cols)):
        col_lcol[lemma_cols[i]]=cols[i]
    col_lcol['*']='*'
    lemma_col='*'
    if len(lemma_cols)> len(lemma_nl): #check every lemmatized column with lemmatized list of non-stop words
        for ele in lemma_cols:
            for elem in lemma_nl:
                if ele==elem:
                    lemma_col=ele
                    return col_lcol[lemma_col]
#returns the first match...assumption is that the first column mentioned will be the select column:
#eg. Show me Sales after... will have sales as the first column.. or show me years...will have years
    else:
        for ele in lemma_nl:
            for elem in lemma_cols:
                if ele==elem:
                    lemma_col=ele
                    return col_lcol[lemma_col]
    return col_lcol[lemma_col] #for returning '*'

#select_col= sel_col()

def wcol(): #where column, also gets inputted in the select statement. SELECT * from table "WHERE" column name= w
    if(named_entity_identifier()==[]): #if NER does not identify an entity
        return 'N/A'
    cols = list(df_sq.columns) #list of columns
    col_values={} #dictionary for mapping values of columns with column name. Eg: {2012:Year, 60000: Sales}
    values=[] #list of values of columns
    for column in cols:
        col_values[str(df_sq[column][1])]=column #m
        values.append((df_sq[column][1]))

    doc_entities=named_entity_identifier()
    doc2 = nlp(str(values))  #nlp on column values
    for ent in doc2.ents:
        for ent1 in doc_entities:
            if ent.label_==ent1.label_: #matching entity of column value and entities in the command statement
                return (col_values[str(ent.text)],ent1.text) #return WHERE column, and value for WHERE expression



#Problem: Have to infer the name of the column from the natural language statement
# Solution: named entity identifier....can modify spaCy module to add more of personalized types for
# our specific dataset

def w_expression(): #generate WHERE expression. WHERE "Sales=2014" or WHERE "Year>=2014"
    #process natural language expression to  give more than, less than or equal to a value (numeric).
    if(wcol()=='N/A'):
        return "N/A"
    value=-1 #default initial value
    operator='=' #default initial value
    op_determinator="" #default initial value of string that determines the operator for the WHERE statement

    if(len(wcol()[1].split())<=1):
        try: #check for int
            value = int(wcol()[1]) #converting str value to int
        except: #if string
            value=wcol()[1]
            operator='='
            return (f"{wcol()[0]}{operator}{value}") #returns where expression for string. value will be string entity identified by NER

    #if num of tokens in entity value is more than one
    else: # will only be "more than 14000" case...string entities will be solved by named entity identifier and
        #will be only one entity as in the string above
        exprn_list=wcol()[1].split() #['more', 'than', '40000']
        for expression in exprn_list:
            try:
                value=int(expression) #value will be the integer value in the expression
            except:
                op_determinator=op_determinator+ expression + ' '



    if(op_determinator==""): #if the entity returned does not have 'more than' in statement
        op_dict = {'after': '>', 'from': '>=', 'before': '<=', 'following':'>', 'prior':'<=', 'over':'>='} #mapping words to corresponding operators
        #dictionary of words to compare similarity
        keys_list=list(op_dict.keys())
        token_list=tokenize() #list of tokens in command statement

        smallest_distance = 100 #initial large value for smallestdist
        op_key = "" #key for operator dictionary
        for key in keys_list:
            dist_list = [] #list of one key with all tokens. eg: ['after', 'show', 'me'.....,'2014']
            dist_list.append(key)
            for token in token_list:
                dist_list.append(token)

            vectorizer = Vectorizer() #initialize vectorizer object.. need to reinitialize everytime new list is run
            vectorizer.run(dist_list)
            vectors_bert = vectorizer.vectors #create vectors list
            count = 1
            sd_count = 1
            #dummy = []
            #print(dist_list)

            for i in range(len(dist_list) - 1):
                if (spatial.distance.euclidean(vectors_bert[0], vectors_bert[count]) < smallest_distance):
                    #the less the distance in the elements, the more similar they are.
                    #check distance between first element(key), with all tokens in command statement
                    smallest_distance = spatial.distance.euclidean(vectors_bert[0], vectors_bert[count])
                    #dummy.append(smallest_distance)
                    op_key=dist_list[0] #operator name with the smallest distance
                count += 1
        operator = op_dict[op_key] #operator symbol from operator dictionary

        '''
        if('after' in tokenize()): #BERT similarity.. convert more than to after
                operator='>'
        elif('from' in tokenize()):
            operator='>='
        elif('before' in tokenize()):#BERT similarity.. convert less than to before
            operator = '<='
        else:
            operator='='
        '''
    else:
        op_dict={'more':'>', 'less':'<', 'equal':'=', 'greater':'>','below':'<','under':'<'}

        dummy_l=[] #dummy lsit to hold first word of op_determinator expression. (i.e. 'more' from "more than")
        dummy_l.append(op_determinator.split(' ')[0])
        distance_list = dummy_l + list(op_dict.keys()) #eg: ['more', 'more', 'less'....'under']
        #holds the first words of op_determinator  and compares it to the keys in the dictionary
        #first word of operator determinator expression will be the key word... MORE than, LESS than, EQUAL to, GREATER than
        vectorizer = Vectorizer()
        vectorizer.run(distance_list)
        vectors_bert = vectorizer.vectors
        smallest_distance=100
        count=1
        sd_count=1
        #try1=[]
        print(distance_list)

        for i in range(len(distance_list)-1):
            if(spatial.distance.euclidean(vectors_bert[0], vectors_bert[count])<smallest_distance):
                smallest_distance=spatial.distance.euclidean(vectors_bert[0], vectors_bert[count])
                #try1.append(smallest_distance)
                sd_count=count
            #try1.append(spatial.distance.euclidean(vectors_bert[0], vectors_bert[count]))
            count += 1
        operator=op_dict[distance_list[sd_count]]

    # operator: <,>,=,<=,>=
    # value: value from natural language (i.e. 2014 in this case)
    return (f"{wcol()[0]}{operator}{value}")


#print(w_expression())
def sqlquery_1(): #generate SQL query
    q = "SELECT * FROM df_sq"
    psql.sqldf(q, globals())
    sq = lambda q: psql.sqldf(q, globals())
    # sq is the new function name, q is the argument (in this case query) passed, and after the ':' is the
    # return of the lambda func that goes to sq.
    where_exp= w_expression()
    where_col=wcol()[0]

    select_col=sel_col()
    #print(select_col, where_col)
    if(where_exp=='N/A'):
        q1= f'SELECT {select_col} FROM df_sq'
        return (sq(q1))
    q1 = f'SELECT {where_col},{select_col} FROM df_sq where {where_exp} '
    return (sq(q1))

q=sqlquery_1()
print(q)

def plot_sql(): #plot SQL query
    dev_x=q[wcol()[0]]
    #dev_x=q[sel_col()]
    dev_y=q[sel_col()]
    #dev_y=q[wcol()[0]]
    plt.bar(dev_x, dev_y,)
    plt.xlabel(wcol()[0])
    plt.ylabel(sel_col())
    plt.autoscale(enable=True, axis='y')
    plt.tight_layout()
    plt.show()


plot_sql()

#function to take input from microphone. Does not work due to errors installing PyAudio
'''
def speech_to_text(): #error installing pyaudio.
    recognizer=sr.Recognizer()
    with sr.Microphone() as sound_source:
        print("What would you want to learn: ")
        audio=recognizer.listen(sound_source)

        try:
            text=recognizer.recognize_google(audio)
            #print(f"You said {text}")
        except:
            text='N/A'
            print("Could not understand what you said.")
    return text
'''


