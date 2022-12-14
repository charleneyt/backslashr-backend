import React from 'react';
import { FormInput } from "shards-react";
import './Homepage.css';
import MenuBar from '../components/MenuBar';
import { getSearchResults } from '../fetcher';
// Homepage
class HomePage extends React.Component {

  constructor(props) {
    super(props)

    this.state = {
      value: ''
    };
    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);


  }

  // Change the text as the user enters them
  handleChange(event) {
    this.setState({ value: event.target.value });
  }

  // Go to the following site after clicking on "Enter" image
  handleSubmit() {
    window.location = '/search?query=' + this.state.value
    getSearchResults()

  }

  render() {
    return (
      <div className="App">
        <MenuBar />
        {/* Title of the page  */}
        <header className="Homepage-header">
          <span>&#60;</span>
          Welcome to Backslash R
          <span>&#62;</span>
        </header>
        {/* Search Bar */}
        <FormInput style={{ fontSize: "30px", color: "#adadad", textAlign: "center", width: "50vw", margin: "auto" }}
          type="text"
          value={this.state.value}
          onChange={this.handleChange}
          placeHolder="Search"
          onKeyPress={(event) => {
            const code = event.keyCode || event.which;
            //13 is the enter keycode
            if (code === 13) {
              this.handleSubmit()
            }
          }}
        />
        {/* Enter Image  */}
        <img style={{ width: 250, height: 150, margin: "auto", marginTop: "2vh" }} src="enter.png" alt="enter" onClick={() => this.handleSubmit()}></img>
      </div>
    )
  }
}

export default HomePage