from flask import Flask, request

import scrapper

app = Flask(__name__)

@app.route('/')
def hello():
    return '<h1>Derenchuk V.</h1>'

@app.route('/dataset/<group_id>')
def dataset(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getDataset(group_id,action_update)

@app.route('/dataset_skill/<group_id>')
def dataset_skill(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getDatasetSkill(group_id,action_update)

@app.route('/employment/<group_id>')
def employment(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getEmployment(group_id, action_update)

@app.route('/closed_message/<group_id>')
def closed_message(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getClosedMessage(group_id, action_update)

@app.route('/skill/<group_id>')
def skill(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getSkill(group_id, action_update)

@app.route('/vacancy/<group_id>')
def vacancy(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getVacancy(group_id, action_update)

@app.route('/city/<group_id>')
def city(group_id):
    action = request.args.get('action')
    action_update = False
    if action == 'update':
        action_update = True
    return scrapper.getCity(group_id, action_update)



if __name__ == '__main__':
    app.run(debug=True, port=5000)