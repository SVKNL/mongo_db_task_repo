from typing import Dict, List, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection


class MongoTaskRepository:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self._client = AsyncIOMotorClient(mongo_uri)
        self._db = self._client[db_name]
        self._collection: AsyncIOMotorCollection = self._db[collection_name]

    async def create_task(self, data: dict) -> str:
        result = await self._collection.insert_one(data)
        return str(result.inserted_id)

    async def get_task_by_id(self, task_id: str) -> Optional[dict]:
        try:
            obj_id = ObjectId(task_id)
        except Exception:
            return None
        document = await self._collection.find_one({"_id": obj_id})
        if document:
            document["_id"] = str(document["_id"])
            return document
        return None

    async def delete_task(self, task_id: str) -> bool:
        try:
            obj_id = ObjectId(task_id)
        except Exception:
            return False
        result = await self._collection.delete_one({"_id": obj_id})
        return result.deleted_count == 1

    async def aggregate_by_tags(self) -> List[Dict]:
        pipeline = [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$project": {"tag": "$_id", "count": 1, "_id": 0}}
        ]

        cursor = self._collection.aggregate(pipeline)

        results = []
        async for doc in cursor:
            results.append(doc)

        return results

    async def close(self):
        self._client.close()

# Testing.
async def main():
    repo = MongoTaskRepository("mongodb://localhost:27017", "task_db", "tasks")
    task_id = await repo.create_task(
        {
            "title": "MongoDB ",
            "tags": ["easy", "educational"],
            "owner": "Stepan Karpov"
        }
    )
    print(f"ID: {task_id}")
    task = await repo.get_task_by_id(task_id)
    print(f"Title: {task}")
    agg_result = await repo.aggregate_by_tags()
    print(f"Aggregation by tags: {agg_result}")
    deleted = await repo.delete_task(task_id)
    print(f"Deleted: {deleted}")
    await repo.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())