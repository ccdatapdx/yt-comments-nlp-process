from file_processing import FileProcess
from yt_channel_process_nlp import SpacyProcessing

# def main_nlp_data():
#     file_processing = FileProcess('Destiny')
#     data = SpacyProcessing()
#     data = data.process_spacy()
#     file_processing.write_json(data)
#     file_processing.write_s3()

def main_nlp_data():
    data = SpacyProcessing()
    data = data.process_spacy()
    




def lambda_handler(event,context):
     event = main_nlp_data()
     return event
