import pandas as pd
from sqlalchemy import create_engine, text, exc
import sqlalchemy.types
from sqlalchemy.orm import sessionmaker
from pathlib import Path

def load_dataset(filepath):
    data = pd.read_csv(filepath)
    return data



def get_db_engine(connection_string):
    engine = create_engine(connection_string, echo=True)
    return engine

def ingest_data(data, engine):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        data.to_sql(
            'ecommerce', 
            engine, 
            if_exists='replace', 
            index=False, 
            method='multi', 
            chunksize=1000,
            
        )
        # Commit the transaction
        session.commit()  # Adding the commit statement here
        print("Data ingested successfully.")
    except exc.SQLAlchemyError as e:
        session.rollback()  # Rollback in case of an error
        print(f"An error occurred during data ingestion: {e}")
    finally:
        session.close()  # Ensure the session is closed

def create_indexes(engine):
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            connection.execute(text('CREATE INDEX IF NOT EXISTS idx_pu_location ON taxi_trips ("PULocationID");'))
            connection.execute(text('CREATE INDEX IF NOT EXISTS idx_do_location ON taxi_trips ("DOLocationID");'))
            trans.commit()
            print("Indexes created successfully.")
        except exc.SQLAlchemyError as e:
            print(f"An error occurred during index creation: {e}")
            trans.rollback()

def main():
    filepath = Path(__file__).parent / "data-yellow-202103.parquet"
    dataset = load_dataset(str(filepath))
    transformed_data = transform_data(dataset)
    
    # Define the connection_string outside the function
    connection_string = "postgresql://postgres:derek@db:5432/my_database"
    engine = get_db_engine(connection_string)
    
    ingest_data(transformed_data, engine)
    create_indexes(engine)

if __name__ == "__main__":
    main()
    
    
