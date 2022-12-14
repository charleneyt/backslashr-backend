import React from 'react';
import {
    Card
} from "shards-react";

// Create a card template for Shopping Results
const ShoppingCard = ({ item }) => {
    // Try getting the following information from Rainforest API
    let title = ""
    let image = ""
    let price = ""
    let link = ""
    try {
        title = item["title"]
        image = item["image"]
        price = item["price"]["value"]
        link = item["link"]
    }
    catch {
        console.log("Failed getting shopping list")
    }
    return (
        <Card className="Center">

            <img
                style={{ width: 300, margin: 25 }}
                src={image}
                onError={({ currentTarget }) => {
                    currentTarget.onerror = null;
                    currentTarget.src = "enter.png";
                }}
            />
            <h6 style={{ color: "#000000" }}>{title}</h6>
            <h3 style={{ color: "#000000" }}>${price} </h3>
            <h6 style={{ color: "#000000" }}><a href={link}>Click to view product</a></h6>
        </Card>
    )
}

export default ShoppingCard