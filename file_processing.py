import boto3
import json
import os
import re
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account


class FileProcess:

    def __init__(self,channel_name:str) -> None:
        self.s3 = boto3.client('s3')
        self.channel_name = channel_name
        self.lambda_dir = '/tmp'
        self.local_cwd = os.getcwd()
        self.recent_file_date = self.S3_recent_string()
             
    def S3_recent_file(self):
        response = self.s3.list_objects_v2(Bucket='yt-channel-comments')
        sorted_objects = sorted(response['Contents'],
                                key=lambda obj: obj['LastModified'],
                                reverse=True)
        most_recent_file = sorted_objects[0]
        file_name = most_recent_file['Key']
        return file_name
    
    def s3_auth(self):
        file_name = 'yt-comments-dashboard-41b385655862.json'
        file_path = f'{self.lambda_dir}/{file_name}'
        self.s3.download_file('auth-ccdatapdx',file_name,file_path)
        return 
    
    def open_S3_recent(self):
        '''
        function to open most recent file from raw comments S3 bucket
        '''
        file_name = self.S3_recent_file()
        file_path = f'{self.lambda_dir}/{file_name}'
        self.s3.download_file('yt-channel-comments',file_name,file_path)
        with open(file_path,'r') as f:
            s3_file = json.load(f)
        return s3_file
        
    def open_S3_recent_metadata(self):
        file = self.open_S3_recent()
        metadata = file['metadata']
        meta_ids = metadata['video_ids']
        meta_dates = metadata['video_date']
        meta_titles = metadata['video_titles']
        meta_keys = list(zip(meta_ids,meta_dates,meta_titles))
        meta_names = ['video_ids','video_dates','video_titles']
        video_metadata = {'meta_keys':meta_keys,'meta_names':meta_names}
        return video_metadata
        
    def S3_recent_string(self):
        '''
        function to get most recent date from recent file
        '''
        file_name = self.S3_recent_file()
        date_pattern = r'\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}'
        match = re.search(date_pattern, file_name)
        date = match.group()
        return date
                             
    def write_s3(self,source_file_string,s3_file_name):
        self.s3.upload_file(source_file_string,'yt-channel-nlp',s3_file_name)
    
    def write_gbq(self,df:pd.DataFrame,destination_table:str):
        self.s3_auth()
        service_account_file = 'yt-comments-dashboard-41b385655862.json'
        credentials = service_account.Credentials.from_service_account_file(
            f'{self.lambda_dir}/{service_account_file}',
        )
        to_gbq = pandas_gbq.to_gbq(df,
                                   destination_table=f'yt_comments_nlp.{destination_table}_{self.channel_name}',
                                   project_id='yt-comments-dashboard',
                                   if_exists='replace',
                                   credentials=credentials
        )
        return to_gbq

    def process_file(self,file_type):    
        nlp_file_name = f'{self.channel_name}_{self.recent_file_date}_{file_type}.json'
        nlp_lambda_string = f'{self.lambda_dir}/{nlp_file_name}'
        self.write_s3(nlp_lambda_string,nlp_file_name)
        return
    
    def write_json(self,file_type,json_data: pd.DataFrame):
        nlp_file_name = f'{self.channel_name}_{self.recent_file_date}_{file_type}.json'
        nlp_lambda_string = f'{self.lambda_dir}/{nlp_file_name}'
        json_data = json_data.to_json(nlp_lambda_string)
        return json_data
