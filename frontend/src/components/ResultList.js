import React from 'react';

// Go to the following page after user click on "Learn More"
function goToMatch(website) {
    window.location = website
}

// Create a list template for Search Results
const ResultCard = ({ item }) => {
    let result = item.split("&&");
    let url = result[0]
    let content = result[1]
    // console.log(url, " ", content)
    return (
        < body >
            <div className='ListGrid'>
                <div className="Left">
                    <div style={{ color: "#8dcee2", textDecoration: 'underline' }} onClick={() => goToMatch(url)}>{url}</div>
                    <div style={{ color: "white", textAlign: "left" }}>
                        {content}
                    </div>
                </div>
            </div>
        </body >
    )
}

export default ResultCard