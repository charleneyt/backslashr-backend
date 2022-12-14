import React from 'react';
import {
    Card,
    CardBody,
    Button
} from "shards-react";

// Go to the following page after user click on "Learn More"
function goToMatch(website) {
    window.location = website
}

// Create a card template for Image Search 
const ImageCard = ({ item }) => {
    // Get the URL and altText
    let split = item.split("&&")
    let url = split[0]
    let altText = split[1]
    return (
        < body >
            <Card className="Center">
                <img
                    style={{ width: 300, margin: 25 }}
                    src={url}
                    alt={altText}
                    onError={({ currentTarget }) => {
                        currentTarget.onerror = null; // prevents looping
                        currentTarget.src = "enter.png";
                    }}
                />
                <CardBody>
                    <Button theme="dark" onClick={() => goToMatch(url)} >Learn more &rarr;</Button>
                </CardBody>
            </Card>
        </body >
    )
}

export default ImageCard