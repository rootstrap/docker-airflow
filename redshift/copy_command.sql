COPY patient
            FROM 's3://patients-records/cleaned/100.csv'
            with credentials
            'aws_access_key_id={ACCESS_KEY};aws_secret_access_key={SECRET_KEY}'
            CSV QUOTE as '"' DELIMITER ','
            ACCEPTINVCHARS 
            
            

