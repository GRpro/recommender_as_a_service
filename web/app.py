from flask import Flask, send_file, request, jsonify
import requests
import json

app = Flask(__name__)

app.config['DEBUG'] = True


@app.route("/")
def index():
    return send_file("index.html")

# REST Api


@app.route('/ratings', methods=['POST'])
def post_ratings():
    body = str(request.data, 'utf-8')
    headers = {'Content-type': 'application/json'}
    r = requests.post('http://localhost:8080/ratings', data=body, headers=headers)
    print(r.status_code)
    print(r.text)
    return 'OK'


@app.route('/movies/<n>', methods=['GET'])
def get_movies(n):
    headers = {'Content-type': 'application/json'}
    r = requests.get('http://localhost:8080/movies/%s' % n, headers=headers)
    return r.content


@app.route('/ratings/<user_id>/top/<n>', methods=['GET'])
def get_top_ratings_for_user(user_id, n):
    headers = {'Content-type': 'application/json'}
    r = requests.get('http://localhost:8080/ratings/%s/top/%s' % (user_id, n), headers=headers)
    return r.content


if __name__ == "__main__":
    app.run(host='0.0.0.0')
