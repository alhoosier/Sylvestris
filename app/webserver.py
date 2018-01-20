from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def homepage():

    app_name = 'Sylvestris: Black Twitter Sentiment Analysis'

    try:
        return render_template('templates/index.html', app_name=app_name)
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    app.run()
