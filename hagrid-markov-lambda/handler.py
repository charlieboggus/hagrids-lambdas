import json
import sys
sys.path.insert(0, 'src/vendor')
import markovify

def lambda_handler(event, context):
    markov_file = open('markov_model.json')
    markov_json = json.load(markov_file)
    model = markovify.Text.from_json(json.dumps(markov_json))
    out = model.make_sentence()
    response = {
        "statusCode": 200,
        "body": out
    }
    return response
