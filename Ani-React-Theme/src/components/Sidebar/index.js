import React, { Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
var GeminiScrollbar = require('react-gemini-scrollbar');
import s from './Sidebar.css';
import SidebarWidgets from './SidebarWidgets';
import MenuBar from './MenuBar/MenuBar';

class Sidebar extends Component {

  render() {
    return (
      <aside className={s.sidebar}>
        <div className={'sidenav-outer ' + s.sidenavOuter}>
          <MenuBar />
          <SidebarWidgets />
        </div>
      </aside>
    );
  }
}

export default withStyles(s)(Sidebar);
