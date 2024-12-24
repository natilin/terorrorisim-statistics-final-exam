from flask import Flask

from app.routes.keyword_search_route import keyword_blueprint
from app.routes.parameters_search_route import statistics_blueprint

app = Flask(__name__)

app.register_blueprint(statistics_blueprint, url_prefix="/api/terrorism")
app.register_blueprint(keyword_blueprint, url_prefix="/api/terrorism/search")

if __name__ == "__main__":
    app.run()