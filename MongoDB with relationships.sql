
db.customers.insertMany([
    { 
        name: "Kowshik", 
        email: "Kowshik@example.com", 
        address: {city: "Chennai", zipcode: "600000" }, 
        phone: "94949494", 
        registration_date: new Date() 
    },
    { 
        name: "Him", 
        email: "him@example.com", 
        address: {city: "Hyderabad", zipcode: "500000" }, 
        phone: "94949495", 
        registration_date: new Date() 
    },
    { 
        name: "her", 
        email: "her@example.com", 
        address: {city: "Bengaluru", zipcode: "300000" }, 
        phone: "94944996", 
        registration_date: new Date() 
    },
    { 
        name: "Them", 
        email: "them@example.com", 
        address: {city: "TVM", zipcode: "200000" }, 
        phone: "45456456", 
        registration_date: new Date() 
    },
    { 
        name: "Us", 
        email: "us@example.com", 
        address: {city: "Madurai", zipcode: "100000" }, 
        phone: "55555555", 
        registration_date: new Date() 
    }
]);

db.orders.insertMany([
    { 
        order_id: "ORD123456", 
        customer_id: 
        order_date: new Date(), 
        status: "shipped", 
        items: [
            { product_name: "Laptop", quantity: 1, price: 1500 }, 
            { product_name: "Mouse", quantity: 2, price: 25 }
        ], 
        total_value: 1550 
    },
    { 
        order_id: "ORD123457", 
        customer_id: ObjectId('67320c549ec744648a0d8191'), 
        order_date: new Date("2023-06-01T14:00:00Z"), 
        status: "pending", 
        items: [
            { product_name: "Tablet", quantity: 1, price: 300 }
        ], 
        total_value: 300 
    },
    { 
        order_id: "ORD123458", 
        customer_id: ObjectId('67320c549ec744648a0d8192'), 
        order_date: new Date("2023-06-10T14:00:00Z"), 
        status: "delivered", 
        items: [
            { product_name: "Keyboard", quantity: 1, price: 100 },
            { product_name: "Monitor", quantity: 1, price: 200 }
        ], 
        total_value: 300 
    },
    { 
        order_id: "ORD123459", 
        customer_id: ObjectId('67320c549ec744648a0d8193'), 
        order_date: new Date("2023-06-20T14:00:00Z"), 
        status: "shipped", 
        items: [
            { product_name: "Smartphone", quantity: 1, price: 800 }
        ], 
        total_value: 800 
    },
    { 
        order_id: "ORD123460", 
        customer_id: ObjectId('67320c549ec744648a0d8194'), 
        order_date: new Date("2023-07-05T14:00:00Z"),
        status: "processing", 
        items: [
            { product_name: "Headphones", quantity: 1, price: 50 },
            { product_name: "Charger", quantity: 1, price: 20 }
        ], 
        total_value: 70 
    }
]);

db.orders.aggregate([
    { $group: { _id: "$customer_id", total_spent: { $sum: "$total_value" } } },
    { $lookup: { from: "customers", localField: "_id", foreignField: "_id", as: "customer" } },
    { $unwind: "$customer" },
    { $project: { name: "$customer.name", total_spent: 1 } }
]);

//  Group Orders by Status
db.orders.aggregate([
    { $group: { _id: "$status", order_count: { $sum: 1 } } }
]);

db.orders.aggregate([
    { $sort: { order_date: -1 } },
    { $group: { _id: "$customer_id", recent_order: { $first: "$$ROOT" } } },
    { $lookup: { from: "customers", localField: "_id", foreignField: "_id", as: "customer" } },
    { $unwind: "$customer" },
    { $project: { name: "$customer.name", email: "$customer.email", order_id: "$recent_order.order_id", total_value: "$recent_order.total_value" } }
]);

db.orders.aggregate([
    { $sort: { total_value: -1 } },
    { $group: { _id: "$customer_id", most_expensive_order: { $first: "$$ROOT" } } },
    { $lookup: { from: "customers", localField: "_id", foreignField: "_id", as: "customer" } },
    { $unwind: "$customer" },
    { $project: { name: "$customer.name", order_id: "$most_expensive_order.order_id", total_value: "$most_expensive_order.total_value" } }
]);
db.orders.aggregate([
    { $match: { order_date: { $gte: new Date(new Date().setMonth(new Date().getMonth() - 1)) } } },
    { $group: { _id: "$customer_id", recent_order: { $first: "$$ROOT" } } },
    { $lookup: { from: "customers", localField: "_id", foreignField: "_id", as: "customer" } },
    { $unwind: "$customer" },
    { $project: { name: "$customer.name", email: "$customer.email", order_date: "$recent_order.order_date" } }
]);
db.orders.aggregate([
    { $match: { customer_id: ObjectId('67320c549ec744648a0d8190') } },
    { $unwind: "$items" },
    { $group: { _id: "$items.product_name", total_quantity: { $sum: "$items.quantity" } } }
]);

db.orders.aggregate([
    { $group: { _id: "$customer_id", total_spent: { $sum: "$total_value" } } },
    { $sort: { total_spent: -1 } },
    { $limit: 3 },
    { $lookup: { from: "customers", localField: "_id", foreignField: "_id", as: "customer" } },
    { $unwind: "$customer" },
    { $project: { name: "$customer.name", total_spent: 1 } }
]);

db.orders.insertOne({
    order_id: "ORD123461",
    customer_id: ObjectId("67320c549ec744648a0d8191"),
    order_date: new Date("2023-08-01T14:00:00Z"),
    status: "pending",
    items: [
        { product_name: "Smartphone", quantity: 1, price: 700 },
        { product_name: "Headphones", quantity: 2, price: 50 }
    ],
    total_value: 800
});

db.customers.aggregate([
    { $lookup: { from: "orders", localField: "_id", foreignField: "customer_id", as: "orders" } },
    { $match: { "orders": { $size: 0 } } },
    { $project: { name: 1, email: 1 } }
]);
db.orders.aggregate([
    { $unwind: "$items" },
    { $group: { _id: "$items.product_name", total_quantity: { $sum: "$items.quantity" } } },
    { $sort: { total_quantity: -1 } },
    { $limit: 1 }
]);

db.orders.aggregate([
    { $group: { _id: "$customer_id", order_count: { $sum: 1 } } },
    { $lookup: { from: "customers", localField: "_id", foreignField: "_id", as: "customer" } },
    { $unwind: "$customer" },
    { $project: { name: "$customer.name", order_count: 1 } }
]);
