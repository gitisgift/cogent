from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from db_configuration import Base, db, app, TableBase
from app.services.logger import Logger
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker,query
from db_layer.models import Client
from sqlalchemy.engine import URL
import os
import pyodbc
from db_layer.models import Client,Users


class DbConnection:
    _instance = None

    def __init__(self):
        self.logger = Logger()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def connect_to_sql_connection_ashutosh_approach(self,server, database, username, password):
        try:
            DSN = 'OD1BC-SQL'  # Name of your Data Source Name
            UID = os.getenv('DB_USER')  # Username
            PWD = os.getenv('DB_PWD')  # Password
            conn = pyodbc.connect('DSN=' + DSN + ';UID=' + UID + ';PWD=' + PWD + ';DATABASE=AudioTrans')
            # engine = create_engine(conn)
            # engine.connect()
            # Example query execution
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dbo.Client")
            # Client = Client.objects.all()
            rows = cursor.fetchall()
            # Do something with the data (print it for demonstration)
            for row in rows:
                print(row)

            # Close connection
            conn.close()
        except Exception as e:
            self.logger.error("connect_to_database", e)
            raise

    def connect_to_sql_connection(self, server, database, username, password):
        try:
            # dns = f'mssql+pyodbc://{server}/{database}?driver=SQL+Server'
            dns = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
            engine = create_engine(dns)
            Session = sessionmaker(bind=engine)
            session = Session()
            clients_data= session.query(Client).filter_by(Id=1).all()
            for user in clients_data:
                print("SubscriptionId:- ",user.ClientUserName)
            clients = session.query(Client).all()
            for user in clients:
                print("ClientName:- ",user.ClientName, "ClientEmail:- ", user.ClientEmail)

            client_info = Client(Id=2,ClientName="sudhir.kumar", ClientEmail="sudhir.kumar@agreeya.net", SubscriptionId="2364_ETYW_45_JH", ModelType ="Small" ,PaymentStatus ="Done"  )
            user_info = Users(Id= 7, Name= "sudhir.kumar", Email= "sudhir.kumar@agreeya.net")
            user_info1 = Users(Id=1, Name="sudhir1.kumar", Email="sudhir1.kumar@agreeya.net")
            user_info2 = Users(Id=2, Name="sudhir2.kumar", Email="sudhir2.kumar@agreeya.net")
            user_info3 = Users(Id=3, Name="sudhir3.kumar", Email="sudhir3.kumar@agreeya.net")
            user_info4 = Users(Id=4, Name="sudhir4.kumar", Email="sudhir4.kumar@agreeya.net")
            user_info5 = Users(Id=5, Name="sudhir5.kumar", Email="sudhir5.kumar@agreeya.net")
            user_info6 = Users(Id=6, Name="sudhir6.kumar", Email="sudhir6.kumar@agreeya.net")
            user_info8 = Users(Id=234, Name="sudhir.kumar", Email="sudhir.kumar@agreeya.net")
            # session.add(user_info)
            # session.add(user_info1)
            # session.add(user_info2)
            # session.add(user_info3)
            # session.add(user_info4)
            # session.add(user_info5)
            # session.add(user_info6)
            #Deleted Code
            user_id = 234  # Replace with the actual ID of the record to delete
            user_to_delete = session.query(Users).get(user_id)
            if user_to_delete is not None:  # Check if the record exists
                session.delete(user_to_delete)  # Add the object to the session for deletion
                # session.commit()  # Commit changes to the database
                print(f"User with ID {user_id} deleted successfully.")
            else:
                print(f"User with ID {user_id} not found.")

            session.commit()
            # with engine.connect() as con:
            #     rs = con.execute(text("select * from dbo.Client"))
            #     for row in rs:
            #         print(row)
            session.close()
        except Exception as e:
            session.close()
            self.logger.error("connect_to_database", e)
            raise
        finally:
            session.close()

    def connect_to_sql_connection_working(self, server, database, username, password):
        try:
            dns = f'mssql+pyodbc://{server}/{database}?driver=SQL+Server'
            engine = create_engine(dns)
            with engine.connect() as con:                
                rs = con.execute(text("select * from dbo.Client"))
                for row in rs:
                    print(row)
        except Exception as e:
            self.logger.error("connect_to_database", e)
            raise
    def connect_to_database(self):
        try:
            # Connect to the database
            #     import sqlalchemy
            #     engine = sqlalchemy.create_engine('sqlite:///D:/Cogent/Cogent-AI/app/Cogent-AI.db')
            #     conn = engine.connect()
            app = Flask(__name__)
            app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///D:/Cogent/AI_Repo/Cogent-AI/app/Cogent-AI.db'
            db = SQLAlchemy(app)
            with app.app_context():
                Base = automap_base()
                Base.prepare(db.engine, reflect=True)
        #         Client = Base.classes.client
        except Exception as e:
            self.logger.error("connect_to_database", e)
            raise

    def close_database_connection(self):
        try:
            # Close the database connection
            print("Database connection closed")
            db.session.close()
            engine_obj = db.get_engine(app)
            engine_obj.dispose()
        except Exception as e:
            self.logger.error("close_database_connection", e)
