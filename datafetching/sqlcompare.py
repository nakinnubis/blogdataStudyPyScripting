from pathlib import Path
import pymysql
import json
import datetime
import yaml

class SqlDataCompare():
    # f"""
    #             SELECT url, userid
    #             FROM user_blog
    #             WHERE processed = 0
    #             AND status = "not_crawled"
    #             AND blogpost_id is null;
    #         """
    def getDataFromTable(self,query,dbConnection):
         connection = dbConnection
         with connection.cursor() as cursor:
             # Getting urls from user_blog
             cursor.execute(query)
             return cursor.fetchall()

    def compareDataFromTables(self,resultFromTableToCompare,dbConnection):
        result = []
        connection = dbConnection
        with connection.cursor() as cursor:
             # Getting urls from user_blog
             for data in resultFromTableToCompare:
                permalink = data['permalink']
                query = f"""SELECT domain,url,author,title,title_sentiment,title_toxicity,published_date,content,content_sentiment,content_toxicity,content_html,language,links,tags FROM blogs.posts where url = '{permalink}';"""
                cursor.execute(query)
                record =  cursor.fetchone()
                # v_dict = version.dict()
                # record["published_date"] = record["published_date"].isoformat()
                if(record is not None):
                    result.append(record)
        return result
        

    def get_connection(self,host,username,password,dbname,charset='utf8mb4',use_unicode=True):
        connection = pymysql.connect(host=host,
                                user=username,
                                password=password,
                                db=dbname,
                                charset=charset,
                                use_unicode=use_unicode,
                                cursorclass=pymysql.cursors.DictCursor)
        return connection
    
    def performQuery(self):
        config = yaml.safe_load(open("./datafetching/settings.yml"))
        databaseone = config["databases"]["databaseone"]
        databasetwo = config["databases"]["databasetwo"]
        connectionOne = self.get_connection(databaseone["host"],databaseone["username"],databaseone["password"],databaseone["dbname"], databaseone['charset'],databaseone["use_unicode"]) 
        connectionTwo = self.get_connection(databasetwo["host"],databasetwo["username"],databasetwo["password"],databasetwo["dbname"], databasetwo['charset'],databasetwo["use_unicode"]) 
        queryDataSourceOne = Path('./datafetching/sqlquery.txt').read_text()
        s = queryDataSourceOne
        blogtrackerData = self.getDataFromTable(s,connectionOne)
        result = self.compareDataFromTables(blogtrackerData,connectionTwo)
        with open('data.json', 'w') as outfile:
            enco = lambda obj: (
                    obj.isoformat()
                    if isinstance(obj, datetime.datetime)
                    or isinstance(obj, datetime.date)
                    else None
                )
            json.dump(result,outfile,default=enco)
    
    def json_serial(self,obj):
    # """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError ("Type %s not serializable" % type(obj))


sql = SqlDataCompare()
sql.performQuery()

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)