from flask import Flask, request, jsonify
import pymongo
import traceback

app = Flask(__name__)


@app.route('/insert_data', methods=['POST'])
def insert_documents():
    try:
        # Extract collection name and documents from the request JSON
        request_data = request.get_json()
        print(request_data)
        collection_name = request_data[0]['collection']
        documents = request_data

        # Connect to MongoDB
        client = pymongo.MongoClient(
            'mongodb://?authMechanism=DEFAULT&authSource=admin')
        db = client['job_scrapers']
        collection = db[collection_name]

        error_occurred = ''
        for document in documents:
            try:
                # Check if the document already has a job_id field
                if 'job_id' not in document:
                    raise ValueError('Document is missing the job_id field.')

                job_id = document['job_id']

                # Insert or update the document in the specified collection
                collection.update_one(
                    {'job_id': job_id},
                    {'$set': document},
                    upsert=True
                )

                # Return a response indicating success
                response = {'message': 'Documents inserted successfully.'}
                return jsonify(response), 200

            except Exception as e:
                traceback.print_exc()
                error_occurred = True  # Update the flag to indicate error

        # Close the MongoDB connection
        client.close()

        if error_occurred:
            # Return an error response if an error occurred
            response = {'error': 'Error inserting documents.'}
            return jsonify(response), 500
        else:
            # Return a response indicating success if no errors occurred
            response = {'message': 'Documents inserted or updated successfully.'}
            return jsonify(response), 200

    except Exception as e:
        traceback.print_exc()
        response = {'error': str(e)}
        return jsonify(response), 500


if __name__ == '__main__':
    # Run the Flask app with debug mode enabled
    app.run(debug=True)
