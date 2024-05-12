import React, { Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './SidebarWidgets.css';
import SidebarProfile from './SidebarProfile';
import SidebarCalendar from './SidebarCalendar';
import SidebarNewsFeed from './SidebarNewsFeed';


class SidebarWidgets extends Component { // eslint-disable-line
  render() {
    return (
      <div className={`sideWidgets ${s.sideWidgets}`}>
        <div className={`widgets-content ${s.widgetsContent}`}>
          <SidebarProfile />
          <SidebarCalendar />
          <SidebarNewsFeed />
        </div>
      </div>
    );
  }
}

export default withStyles(s)(SidebarWidgets);
