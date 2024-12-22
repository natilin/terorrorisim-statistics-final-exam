from flask import Flask

from app.routes.statistic_route import statistics_blueprint

app = Flask(__name__)

app.register_blueprint(statistics_blueprint, url_prefix="/api/terrorism")

if __name__ == "__main__":
    app.run()