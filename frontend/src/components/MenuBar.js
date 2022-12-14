import React from 'react';
import {
  Navbar,
  Nav,
  NavItem,
  NavLink
} from "shards-react";

// Creates the menu bar with "Home", "Image", and "Shopping"
class MenuBar extends React.Component {
  render() {
    return (
      <Navbar type="dark" theme="dark" expand="md">
        <Nav navbar>
          <NavItem>
            <NavLink active href="/">
              Home
            </NavLink>
          </NavItem>
          <NavItem>
            <NavLink active href="/image-search">
              Image
            </NavLink>
          </NavItem>
          <NavItem>
            <NavLink active href="/shopping">
              Shopping
            </NavLink>
          </NavItem>
        </Nav>
      </Navbar>
    )
  }
}

export default MenuBar
