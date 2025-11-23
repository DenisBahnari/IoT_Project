from flask import Flask, request, jsonify

mlServer = Flask(__name__)

@mlServer.route('/train', methods=['POST'])
def train_model():
    data = request.get_json()
    # Placeholder for training logic
    print("Training model with data",flush=True)
    return jsonify({"status": "success", "message": "Model trained!"})


if __name__ == '__main__':
    mlServer.run(host='0.0.0.0', port=5000)
