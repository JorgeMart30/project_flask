import traceback
from datetime import datetime

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError

uri_connect = "postgresql+pg8000://{0}:{1}@{2}:{3}/{4}".format(
    'postgres',
    'prueba1234',
    '172.17.0.1',
    # '34.65.92.100',     # Public instance IP. IP permit: my PC IP
    '5432',
    'db_py'
)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = uri_connect
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class Film(db.Model):
    """
        - Clase para el manejo de la tabla 'films'
    """
    __tablename__ = "films"

    show_id = db.Column(db.String(), primary_key=True)
    insert_table = db.Column(db.DateTime(), nullable=False, default=datetime.now())

    type = db.Column(db.String(), nullable=True)
    title = db.Column(db.String(), nullable=False)

    director = db.Column(db.String(), nullable=True)
    cast = db.Column(db.String(), nullable=True)
    country = db.Column(db.String(), nullable=True)
    release_year = db.Column(db.String(), nullable=True)

    rating = db.Column(db.String(), nullable=True)
    duration = db.Column(db.String(), nullable=True)
    listed_in = db.Column(db.String(), nullable=True)

    description = db.Column(db.String(), nullable=True)

    def __repr__(self):
        return self.title

    def get_to_dict(self):
        dict_to_return = {
            "show_id": self.show_id,
            "insert_table": self.insert_table,
            "type": self.type,
            "title": self.title,
            "director": self.director,
            "cast": self.cast,
            "country": self.country,
            "release_year": self.release_year,
            "rating": self.rating,
            "duration": self.duration,
            "listed_in": self.listed_in,
            "description": self.description
        }
        return dict_to_return


@app.before_first_request
def init_bbdd():
    db.create_all()


@app.route('/')
def ping():
    return "Pong!!"


@app.route('/get_all_data')
def get_all_info():
    try:
        list_film = [each_item.get_to_dict() for each_item in Film.query.all()]
        return jsonify({'_return': 'OK', '_total_films': len(list_film), 'data': list_film})
    except:
        return jsonify({'_return': 'KO', 'message': 'Unexpected exception'})


@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        data_in = request.json
        if 'show_id' in data_in.keys() and 'title' in data_in.keys():
            new_data = Film(
                show_id=data_in['show_id'] if 'show_id' in data_in.keys() else '-',
                type=data_in['type'] if 'type' in data_in.keys() else '-',
                title=data_in['title'] if 'title' in data_in.keys() else '-',
                director=data_in['director'] if 'director' in data_in.keys() else '-',
                cast=data_in['cast'] if 'cast' in data_in.keys() else '-',
                country=data_in['country'] if 'country' in data_in.keys() else '-',
                release_year=data_in['release_year'] if 'release_year' in data_in.keys() else '-',
                rating=data_in['rating'] if 'rating' in data_in.keys() else '-',
                duration=data_in['duration'] if 'duration' in data_in.keys() else '-',
                listed_in=data_in['listed_in'] if 'listed_in' in data_in.keys() else '-',
                description=data_in['description'] if 'description' in data_in.keys() else '-',
            )
        else:
            return jsonify({'_return': 'KO - 201', 'message': 'Violation of required fields'})
        db.session.add(new_data)
        db.session.commit()
        return jsonify({'_return': 'OK - 200'})

    except IntegrityError:
        return jsonify({'_return': 'KO - 202', 'message': 'EXC - IntegrityError'})

    except:
        traceback.print_exc()
        return jsonify({'_return': 'KO', 'message': 'Unexpected exception'})


@app.route('/drop_table')
def drop_table():
    try:
        db.drop_all()
        db.create_all()
        return jsonify({'_return': 'OK'})
    except:
        return jsonify({'_return': 'KO', 'message': 'Unexpected exception'})


@app.route('/get_one_data/<string:show_id_in>')
def get_by_id(show_id_in):
    try:
        film = Film.query.filter_by(show_id=show_id_in).first()
        dict_film = film.get_to_dict()
        return jsonify({'_return': 'OK', 'data': dict_film})
    except:
        return jsonify({'_return': 'KO', 'message': 'Unexpected exception'})


@app.route('/get_contain_title/<string:title_in>')
def get_by_contain_title(title_in):
    try:
        title_to_look = '%{0}%'.format(title_in)
        lists_film = Film.query.filter(Film.title.like(title_to_look))
        list_dict = [each_item.get_to_dict() for each_item in lists_film]
        return jsonify({'_return': 'OK', '_total_films': len(list_dict), 'data': list_dict})
    except:
        return jsonify({'_return': 'KO', 'message': 'Unexpected exception'})


if __name__ == '__main__':
    app.run(port=4000, debug=True)
