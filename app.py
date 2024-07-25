from flask import Flask, request, jsonify,render_template,send_from_directory
import requests
import json
import os

app = Flask(__name__)

@app.route('/')
def home():
    return send_from_directory(os.getcwd(),'index.html')

# Endpoint to receive user input and process it
@app.route('/process', methods=['POST'])
def process():
    input_data = request.json.get('input_data')
    
    # Send the input data to Databricks for processing
    databricks_url = "https://adb-1145123843843530.10.azuredatabricks.net/api/2.0/jobs/run-now"
    headers = {
        "Authorization": "dapie8d22988ce063894e9559568f2278b4a-2",
        "Content-Type": "application/json"
    }
    payload = {
        "job_id": 532836719147641,  # Replace with your Databricks job ID
        "notebook_params": {
            "input_data": json.dumps(input_data)
        }
    }

    response = requests.post(databricks_url, headers=headers, json=payload)
    if response.status_code == 200:
        run_id = response.json().get('run_id')
        
        # Poll for the result
        result_url = f"https://adb-1145123843843530.10.azuredatabricks.net/api/2.0/jobs/runs/get?run_id={run_id}"
        result_response = requests.get(result_url, headers=headers)
        result = result_response.json().get('notebook_output', {}).get('result')
        return jsonify({'result': result})
    else:
        return jsonify({'error': 'Databricks job failed'}), 500

if __name__ == '__main__':
    app.run(debug=True)