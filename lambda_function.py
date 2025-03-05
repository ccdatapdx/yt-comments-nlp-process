from yt_channel_process_nlp import SpacyProcessing

    
def lambda_handler(event,context):
     
     event = SpacyProcessing()
     event = event.process_spacy()
     return event
