COPY patient
            FROM 's3://patients-records/cleaned/test_enter.csv'
            with credentials
            'aws_access_key_id= AKIASPKYDZ4OKYSP2CVO;aws_secret_access_key=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'
            CSV QUOTE as '"' DELIMITER ','
            ACCEPTINVCHARS 
            TRUNCATECOLUMNS
            

COPY patient
            FROM 's3://patients-records/cleaned/1002.csv'
            with credentials
            'aws_access_key_id= AKIASPKYDZ4OKYSP2CVO;aws_secret_access_key=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'
            CSV QUOTE as '"' DELIMITER ','
            ACCEPTINVCHARS 
            
            

