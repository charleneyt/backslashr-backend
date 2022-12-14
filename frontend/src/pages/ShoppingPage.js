import React from 'react';
import './Homepage.css';
import MenuBar from '../components/MenuBar';
import ShoppingCard from '../components/ShoppingCard';
import { getShopping } from '../fetcher'
import {
    FormInput,
} from "shards-react";
import {
    Pagination,
    Divider,
    Spin
} from 'antd'

// Shopping page
class ShoppingPage extends React.Component {

    constructor(props) {
        super(props)

        this.state = {
            minValue: 0,
            maxValue: 8,
            results: [],
            spin: false
        }
        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleInput = this.handleInput.bind(this);
    }

    // Update page as user navigates through the results
    handleChange = value => {
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

    }

    // Go to the following site after clicking on "Enter" image
    handleSubmit() {
        if (this.state.query !== "") {
            this.handleSpinChange(true)
            getShopping(this.state.query).then(res => {
                this.setState({ results: res["search_results"] })
                this.handleSpinChange(false)
            })
        }
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

    render() {
        return (
            <div className="App">
                <MenuBar />
                <header className="Searchresult-header">
                    {/* Title of the page  */}
                    <span>&#60;</span>
                    Shopping
                    <span>&#62;</span>
                    <Divider />
                    <div className='SearchGrid'>
                        {/* Shows the user what they've searched  */}
                        <FormInput style={{ fontSize: "30px", color: "#adadad", textAlign: "center" }}
                            type="text"
                            placeholder="Search"
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
                    <Divider />
                    {/* Display the results in card format */}
                    {this.state.results == null ? <><div>No results to display</div></> :
                        <div style={{ margin: '0 auto', marginTop: '2vh', marginBottom: '2vh' }}>
                            <div className="CardGrid" >
                                {this.state.results.slice(this.state.minValue, this.state.maxValue).map(function (item, i) {
                                    return (
                                        <ShoppingCard key={i} item={item}> </ShoppingCard>
                                    )
                                })}
                            </div>
                        </div>}
                    {/* Page navigation */}
                    <div className="Pagination">
                        <Pagination
                            defaultCurrent={1}
                            defaultPageSize={8}
                            total={this.state.results == null ? 0 : this.state.results.length}
                            onChange={this.handleChange}
                            showSizeChanger={false}
                        />
                    </div>
                </header>
            </div>
        )
    }

}

export default ShoppingPage

