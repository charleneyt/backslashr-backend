import React from 'react';
import './Homepage.css';
import MenuBar from '../components/MenuBar';
import ResultList from '../components/ResultList';
import { getSearchResults } from '../fetcher'
import {
    FormInput,
} from "shards-react";
import {
    Divider,
    Spin
} from 'antd'
import InfiniteScroll from 'react-infinite-scroll-component';

// Search results page
class SearchResults extends React.Component {

    constructor(props) {
        super(props)
        console.log("window location search: ", window.location.search)
        console.log("decoded: ", decodeURI(window.location.search))

        this.state = {
            query: window.location.search ? decodeURI(window.location.search.substring(1).split('=')[1]) : "",
            minValue: 0,
            maxValue: 5,
            items: [],
            results: [],
            length: 0,
            hasMore: true,
            spin: false
        }
        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleInput = this.handleInput.bind(this);
    }

    // Update page as user navigates through the results
    handleChange = value => {
        console.log("from page change: ", value)
        if (value <= 1) {
            this.setState({
                minValue: 0,
                maxValue: 8
            });
        }
        else {
            this.setState({
                minValue: (value - 1) * 8,
                maxValue: value * 8
            });
        }
    }

    // Get all the results from backend and set the results variable
    componentDidMount() {
        if (this.state.query !== "") {
            this.handleSpinChange(true)

            getSearchResults(this.state.query).then(res => {
                // console.log("From component did mount: " + res)
                for (let i = 0; i < res.length; i++) {
                    this.state.results[i] = res[i]["URL"] + "&&" + res[i]["content"]
                    this.handleSpinChange(false)
                }

                this.setState({ length: this.state.results.length })
                this.setState({ items: this.state.results.slice(this.state.minValue, this.state.maxValue) })

                this.fetchData()
            })
        }
    }


    // Go to the following site after clicking on "Enter" image
    handleSubmit() {
        window.location = '/search?query=' + this.state.query
        getSearchResults()
    }

    // Change the text as the user enters them
    handleInput(event) {
        this.setState({ query: event.target.value });
    }

    // Update the spin to reflect that the data is loading
    handleSpinChange(event) {
        this.setState({ spin: event })
    }

    clearForm() {
        this.setState({ query: "" });
    }

    // Get new data for the infinite scroll
    fetchData = () => {
        this.setState({ minValue: this.state.maxValue })
        this.setState({ maxValue: this.state.maxValue + 5 })
        let newItems = this.state.results.slice(this.state.minValue, this.state.maxValue)

        if (this.state.items.length >= this.state.length) {
            this.setState({ hasMore: false })
            this.handleSpinChange(false)
        }

        this.setState({ items: [...this.state.items, ...newItems] })

    }

    render() {
        return (
            <div className="App">
                <MenuBar />
                <header className="Searchresult-header">
                    {/* Title of the page  */}
                    <span>&#60;</span>
                    Search Results
                    <span>&#62;</span>
                    <Divider />
                    <div className='SearchGrid'>
                        {/* Shows the user what they've searched  */}
                        <FormInput style={{ fontSize: "30px", color: "#adadad", textAlign: "center" }}
                            type="text"
                            placeholder={this.state.query}
                            onClick={() => this.clearForm()}
                            onChange={this.handleInput}
                            onKeyPress={(event) => {
                                const code = event.keyCode || event.which;
                                //13 is the enter keycode
                                if (code === 13) {
                                    this.handleSubmit()
                                }
                            }}
                        />
                    </div>
                    <div className='SearchGrid'>
                        {/* Enter Image  */}
                        <img style={{ width: 100, height: 70 }} src="enter.png" alt="enter" onClick={() => this.handleSubmit()}></img>
                    </div>
                    <Divider />
                    <div className='SearchNumber' style={{ textAlign: "left" }}>{this.state.results.length} results total </div>
                    <Spin spinning={this.state.spin} style={{ width: "5%" }} />

                    {/* Using infinite scroll */}
                    <InfiniteScroll
                        dataLength={this.state.items.length}
                        next={this.fetchData}
                        hasMore={this.state.hasMore}
                        loader={<h4>Loading...</h4>}
                        endMessage={
                            <p style={{ textAlign: 'center' }}>
                                <p>End of search results</p>
                            </p>
                        }
                    >
                        {this.state.items.map((item) => (
                            <ResultList item={item}> </ResultList>
                        ))}
                    </InfiniteScroll>
                </header >
            </div >
        )
    }

}

export default SearchResults

