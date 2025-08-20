import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from SQL_table_creation import (
    TestMatch, TestDelivery, ODIDelivery, ODIMatch, 
    T20Delivery, T20Match, IPLDelivery, IPLMatch, DB_link
)

engine = create_engine(DB_link)
Session = sessionmaker(bind=engine)
session = Session()

def clean_dataframe(df: pd.DataFrame):
    df = df.astype(object)
    df = df.map(lambda x: None if pd.isna(x) else x)
    
    # Safe date handling
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = df['date'].apply(lambda x: x.date() if not pd.isna(x) else None)
    
    return df

def load_matches_deliveries(match_csv, delivery_csv, MatchModel, DeliveryModel):
    try:
        # Load and clean data
        match_df = clean_dataframe(pd.read_csv(match_csv))
        print(f'{MatchModel.__tablename__} NaN check:', match_df.isna().sum().sum())
        
        delivery_df = clean_dataframe(pd.read_csv(delivery_csv))
        print(f'{DeliveryModel.__tablename__} NaN check:', delivery_df.isna().sum().sum())
        
        # Convert to ORM objects
        match_records = [MatchModel(**row.to_dict()) for _, row in match_df.iterrows()]
        delivery_records = [DeliveryModel(**row.to_dict()) for _, row in delivery_df.iterrows()]
        
        # Insert matches first and commit
        session.bulk_save_objects(match_records)
        session.commit()
        print(f'Inserted {len(match_records)} rows into {MatchModel.__tablename__}')
        
        # Then insert deliveries (after matches are committed)
        session.bulk_save_objects(delivery_records)
        session.commit()
        print(f"Inserted {len(delivery_records)} rows into {DeliveryModel.__tablename__}")
        
    except Exception as e:
        session.rollback()
        print(f"Error loading {MatchModel.__tablename__}: {e}")
        raise

if __name__ == '__main__':
    try:
        load_matches_deliveries("output/test_matches.csv", "output/test_deliveries.csv", TestMatch, TestDelivery)
        load_matches_deliveries("output/odi_matches.csv", "output/odi_deliveries.csv", ODIMatch, ODIDelivery)
        load_matches_deliveries("output/t20_matches.csv", "output/t20_deliveries.csv", T20Match, T20Delivery)
        load_matches_deliveries("output/ipl_matches.csv", "output/ipl_deliveries.csv", IPLMatch, IPLDelivery)
        print("All CSVs loaded into MySQL successfully")
    except Exception as e:
        print(f"Failed to load data: {e}")
    finally:
        session.close()