from yt_channel_process_nlp import SpacyProcessing

def main_nlp_data():
    data = SpacyProcessing()
    data = data.process_spacy()
    
def lambda_handler(event,context):
     event = main_nlp_data()
     return event
