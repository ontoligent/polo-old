'''
Polo uses mallet to score -- to score documents, that is.

To do:
-- Replace os.system() calls with subprocess calls
-- Convert the whole thing into an object
-- Add convenience functions to create project directories and trial directories
-- Add information from xml files; requires altering the data model
-- Remove print statements from functions
-- Convert into an executible without having to call python; #! depends on server
-- Config file should really be a schema defined XML files, so you can tell if the file has the right content;
   or else a database with a TRIAL table. Config files are fine to process but lack control.
'''

import sys, os, re, configparser, sqlite3, codecs
from lxml import etree

class Polo:

    def __init__(self,project,trial):
        self.project = project
        self.trial = trial
        self.project_path = 'projects/%s' % self.project
        self.trial_path = '%s/trials/%s' % (self.project_path,self.trial)
        self.import_config()
        self.init_mallet()

    def import_config(self):
        cfg_file = 'projects/%s/config.ini' % self.project
        if os.path.exists(cfg_file):
            self.cfg = configparser.ConfigParser()
            self.cfg.read(cfg_file)
            return 1
        else: return 0

    def init_mallet(self):
        self.mallet = {'import-file':{}, 'train-topics':{}}
        self.mallet['import-file']['extra-stopwords'] = '%s/corpus/extra-stopwords.txt' % self.project_path
        self.mallet['import-file']['input'] = '%s/corpus/corpus.csv' % self.project_path
        self.mallet['import-file']['output'] = '%s/corpus.mallet' % self.trial_path
        self.mallet['import-file']['keep-sequence'] = '' # Delete key to remove option
        self.mallet['import-file']['remove-stopwords'] = '' # Delete key to remove option
        user_args = ['num-topics','num-top-words','num-iterations','optimize-interval','num-threads']
        for arg in user_args: self.mallet['train-topics'][arg] =  self.cfg[self.trial][arg]
        self.mallet['train-topics']['input'] = self.mallet['import-file']['output']
        self.mallet['train-topics']['output-topic-keys'] = '%s/model-topic-keys.txt' % self.trial_path
        self.mallet['train-topics']['output-doc-topics'] = '%s/model-doc-topics.txt' % self.trial_path
        #self.mallet['train-topics']['topic-word-weights-file'] = '%s/model-topic-word-weights.txt' % self.trial_path
        self.mallet['train-topics']['word-topic-counts-file'] = '%s/model-word-topic-counts.txt' % self.trial_path
        self.mallet['train-topics']['xml-topic-report'] = '%s/model-topic-report.xml' % self.trial_path
        self.mallet['train-topics']['xml-topic-phrase-report'] = '%s/model-topic-phrase-report.xml' % self.trial_path

    def mallet_run_command(self,op):
        my_cmd = self.cfg['DEFAULT']['mallet_path'] + ' %s' % op
        for arg in self.mallet[op]: my_cmd += ' --%s %s' % (arg,self.mallet[op][arg])
        #print('HEY Running:', my_cmd)
        self.cmd_response = os.system(my_cmd)

    def mallet_import(self):
        self.mallet_run_command('import-file')

    def mallet_train(self):
        self.mallet_run_command('train-topics')

    def create_table_defs(self):
        n = self.mallet['train-topics']['num-topics']
        self.tbl_defs = {
            'doc':{'fkeys':(),'defs':{}},
            'topic':{'fkeys':(),'defs':{}},
            'doctopic':{'fkeys':(),'defs':{}},
            'wordtopic':{'fkeys':(),'defs':{}},
            'topicphrase':{'fkeys':(),'defs':{}},
            #'topicword':{'fkeys':(),'defs':{}}
            }
        self.tbl_defs['doc']['fkeys'] = ('doc_id', 'doc_label', 'doc_content')
        self.tbl_defs['doc']['defs'] = { 'doc_id': 'TEXT', 'doc_label': 'TEXT', 'doc_content': 'TEXT' }
        self.tbl_defs['topic']['fkeys'] = ('topic_id','topic_alpha','topic_words','total_tokens')
        self.tbl_defs['topic']['defs'] = { 'topic_id': 'TEXT', 'topic_alpha': 'REAL', 'total_tokens': 'INTEGER', 'topic_words': 'TEXT' }
        self.tbl_defs['doctopic']['fkeys'] = ('doc_id','doc_label','_topics_')
        self.tbl_defs['doctopic']['defs'] = { 'doc_id': 'TEXT', 'doc_label': 'TEXT', '_topics_': 'REAL' }
        self.tbl_defs['wordtopic']['fkeys'] = ('word_id', 'word_str', '_topics_')
        self.tbl_defs['wordtopic']['defs'] = { 'word_id': 'INTEGER', 'word_str': 'TEXT', '_topics_': 'INTEGER' }
        self.tbl_defs['topicphrase']['fkeys'] = ('topic_id', 'topic_phrase','phrase_count', 'phrase_weight')
        self.tbl_defs['topicphrase']['defs'] = {'topic_id': 'TEXT', 'topic_phrase': 'TEXT','phrase_count': 'INTEGER', 'phrase_weight': 'REAL'}
        #self.tbl_defs['topicword']['fkeys'] = ( 'word_str', '_topics_')
        #self.tbl_defs['topicword']['defs'] = {'word_str': 'TEXT', '_topics_': 'REAL'}
        self.tbl_sql = {}
        for table in self.tbl_defs:
            self.tbl_sql[table] = "CREATE TABLE IF NOT EXISTS %s (" % table
            fields = []
            for field in self.tbl_defs[table]['fkeys']:
                ftype = self.tbl_defs[table]['defs'][field]
                if field == '_topics_':
                    for x in range(int(n)): fields.append('t%s %s' % (x,ftype))
                else: fields.append("'%s' %s" % (field,ftype))
            self.tbl_sql[table] += ','.join(fields)
            self.tbl_sql[table] += ")"

    def import_model(self):
        n = self.mallet['train-topics']['num-topics']
    
        srcfiles = {'csv': {}, 'xml': {}}
        srcfiles['csv']['doc'] = self.mallet['import-file']['input']
        srcfiles['csv']['topic'] = self.mallet['train-topics']['output-topic-keys']
        srcfiles['csv']['doctopic'] = self.mallet['train-topics']['output-doc-topics']
        srcfiles['csv']['wordtopic'] = self.mallet['train-topics']['word-topic-counts-file']
        srcfiles['xml']['topicphrase'] = self.mallet['train-topics']['xml-topic-phrase-report']
        #srcfiles['topicword'] = self.mallet['train-topics']['topic-word-weights-file']
        
        db_file = 'projects/%s/trials/%s/%s-%s.db' % (self.project,self.trial,self.project,self.trial)
        with sqlite3.connect(db_file) as conn:
            cur = conn.cursor()
                    
            # Import the CSV files
            for table in srcfiles['csv']:
                print('HEY Loading table',table)

                # Drop or truncate the table
                cur.execute('DROP TABLE IF EXISTS %s' % table)
                cur.execute(self.tbl_sql[table])
                conn.commit()
                
                # Open the source file
                src_file = srcfiles['csv'][table]
                #with codecs.open(src_file, "r", encoding='utf-8', errors='ignore') as src_data:
                with open(src_file,'r') as src_data:
                    print('HEY Loading csv file',src_file)
                
                    # Handle special case of topicword
                    '''
                    if table == 'topicword':
                        weights = {}
                        my_field_str = 'word_str'
                        for i in range(int(n)):
                            field_str += ',t'+str(i)
                        for line in src_data.readlines():
                            line = line.strip()
                            row = line.split('\t')
                            #topic_id = row[0] # Not used; instead we use the index of the array defined for each word
                            word_str = row[1]
                            word_wgt = row[2]
                            if word_str not in weights.keys(): weights[word_str] = []
                            weights[word_str].append(word_wgt)
                        for word_str in weights.keys():
                            wgt_str = ','.join(weights[word_str])
                            # This now needs a field_str since col order is not guaranteed
                            sql = "INSERT INTO topicword (%s) VALUES ('%s',%s)" % (field_str,word_str,wgt_str)
                            cur.execute(sql)
                        conn.commit() # Do this outside of preceding for loop
                        continue
                    '''
    
                    # Create the field_str for use in the SQL statement
                    fields = []
                    #for (field,ftype) in tbl_cfg.items(table):
                    for field in self.tbl_defs[table]['fkeys']:
                        ftype = self.tbl_defs[table]['defs'][field]
                        if field == '_topics_':
                            for i in range(int(n)): fields.append('t'+str(i))
                        else: fields.append(field)
                    field_str = ','.join(fields)
                    print('HEY Fields:',field_str)
                        
                    # Generate the value string, then insert
                    for line in src_data.readlines():
                        if (re.match('^#',line)): continue
                        line = line.strip()
                        values = [] # Used to create the value_str in the SQL statement
    		
                        if table == 'doctopic':
                            row = line.split('\t')
                            info = row[1].split(',') 
                            values.append(info[0]) # doc_id
                            values.append(info[1]) # doc_label
                            tws = []
                            for i in range(int(n)): tws.append(0)
                            for i in range(2,int(n)*2,2): 
                                tn = int(row[int(i)])
                                tw = row[int(i)+1]
                                tws[tn] = tw
                            for tw in tws:
                                values.append(tw)
    		
                        elif table == 'wordtopic':
                            row = line.split(' ')
                            values.append(row[0]) # word_id
                            values.append(row[1]) # word_str
                            counts = {} # word_counts
                            for x in row[2:]:
                                y = x.split(':') # y[0] = topic num, y[1] = word count
                                counts[str(y[0])] = y[1]
                            for i in range(int(n)):
                                tn = str(i)
                                if tn in counts.keys(): values.append(counts[tn])
                                else: values.append(0)
    
                        elif table == 'topic':
                            row = line.split('\t')
                            values.append('t%s' % row[0]) # topic_id
                            values.append(row[1]) # topic_alpha
                            values.append(row[2]) # topic_list
                            values.append(0) # Place holder for total_tokens until XML file is handled
    		                          
                        #elif table == 'topicword':
                        #    continue # This is handled above
    		
                        elif table == 'doc':
                            row = line.split(',')
                            values.append(row[0]) # doc_id
                            values.append(row[1]) # doc_label
                            values.append(row[2]) # doc_content
                        
                        args = []
                        for i in range(len(values)): args.append('?')
                        arg_str = ','.join(args) 
                        sql2 = 'INSERT INTO `%s` (%s) VALUES (%s)' % (table,field_str,arg_str)
                        cur.execute(sql2,values)
    		
                    conn.commit() # Commit after each table
            
            for table in srcfiles['xml']:
                print('HEY Loading table',table)
                if table == 'topicphrase':
                    # Drop or truncate the table and then create it again
                    cur.execute('DROP TABLE IF EXISTS %s' % table)
                    cur.execute(self.tbl_sql[table])
                    conn.commit()
                    src_file = srcfiles['xml'][table]
                    with open(src_file) as fd:
                        print('HEY Parsing xml file', src_file)
                        tree = etree.parse(fd)
                        for topic in tree.xpath('/topics/topic'):
                            topic_id = 't'+topic.xpath('@id')[0]
                            total_tokens = topic.xpath('@totalTokens')[0]
                            sql1 = "UPDATE topic SET total_tokens = ? WHERE topic_id = ?"
                            cur.execute(sql1,[total_tokens,topic_id])                            
                            for phrase in topic.xpath('phrase'):
                                phrase_weight = phrase.xpath('@weight')[0]
                                phrase_count = phrase.xpath('@count')[0]
                                topic_phrase = phrase.xpath('text()')[0]
                                sql2 = 'INSERT INTO topicphrase (topic_id,topic_phrase,phrase_count,phrase_weight) VALUES (?,?,?,?)'
                                cur.execute(sql2,[topic_id,topic_phrase,phrase_count,phrase_weight])        
                    conn.commit()
                    
            cur.close()
        return 1

if __name__ == '__main__':

    # Get arguments -- expecting project and trial
    if (len(sys.argv) != 3): print('Wrong number of arguments. You need two (project and trial)'); sys.exit(0)
    project = sys.argv[1]
    trial = sys.argv[2]
    print('OK Project:',project)
    print('OK Trial:',trial)

    # Do these directories exist, etc?
    if os.path.exists("projects/%s" % project): print('OK Project directory exists')
    else: print('NOT OK Project directory does not exist. Create one under projects'); sys.exit(0)
    if os.path.exists('projects/%s/trials/%s' % (project,trial)): print('OK Trial directory exists')
    else: print('NOT OK Trial directory does not exist. Create one under your pojects directory'); sys.exit(0)

    # Do the configs exist?
    config_file = 'projects/%s/config.ini' % (project)
    if os.path.exists(config_file): print('OK Config file exists')
    else: print('NOT OK Config file does not exist. Create config.ini in your project directory'); sys.exit(0)
    config = configparser.ConfigParser()
    config.read(config_file)
    #print('OK Config sections (trials):',config.sections())

    # Check if everything is defined ...
    if trial in config.keys(): print('OK Trial defined in config.ini')
    else: print('NOT OK Trial not defined in config.ini'); sys.exit()

    # Create the Polo object
    print('HEY Creating Polo object')
    p = Polo(project,trial)
    
    # CORPUS -> MALLET

    # Run mallet to create the mallet file
    print('HEY Importing mallet file')
    p.mallet_import()
    
    # Run mallet to generate the model
    print('HEY Training topics')
    p.mallet_train()

    # MALLET -> SQLITE
    
    # Generate the SQL
    print('HEY Generating the SQL')
    # Check of the files are there -- stopwords, corpus
    p.create_table_defs()

    # Do the imports
    print('HEY Importing the model')
    p.import_model()

    print('BYE Done with everything')
    sys.exit(0)
