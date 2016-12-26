# -*- coding:utf-8 -*-
import json

from flask import Flask, render_template

app = Flask(__name__)



@app.route('/querys/<int:userId>/')
def query_detail(userId):
    data = json.load(file('data/result.json'))
    print userId
    return render_template('query_detail.html', data=data)


@app.route('/')
def hello_world():
    return app.send_static_file('index.html')

if __name__ == '__main__':
    app.run()
