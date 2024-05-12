import React, { Component } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import FullCalendar from 'rc-calendar/lib/FullCalendar';
import Select from 'rc-select';
import moment from 'moment';
import { FormattedMessage } from 'react-intl';
// import $ from 'jquery';
// import fullcalendar from 'fullcalendar';
import s from './SidebarWidgets.css';

const now = moment();

class SidebarCalendar extends Component {
  componentDidMount() {
    // console.log('this.menuCalendar', this.menuCalendar);
    // $(this.menuCalendar).fullcalendar({
    //   contentHeight: 'auto',
    //   header: {
    //     left: 'title',
    //     right: 'prev,next',
    //     center: '',
    //   },
    // });
    // $(this.sidebarCalendar).fullcalendar();
      // $('#gaurav123').fullCalendar({
      //   contentHeight: 'auto',
      //   header: {
      //     left: 'title',
      //     right: 'prev,next',
      //     center: '',
      //   },
      // });
  }
  render() {
    return (
      <div className={s.calContainer}>
        <div className={s.calHeader}>
          <FormattedMessage
            id="fullcalendar"
            defaultMessage="Full Calendar"
          />
        </div>
        <div>
          <FullCalendar
            style={{ margin: '0px 0px', border: 'none' }}
            Select={Select}
            fullscreen={false}
            default={now}
          />
        </div>
      </div>
    );
  }
}

// <div id="sidebarCalendar" ref={(c) => { this.sidebarCalendar = c; }}> SidebarCalendar </div>
export default withStyles(s)(SidebarCalendar);


      // <div className={`calendar-container text-center ${s.calendarContainer}`}>
      //   <div ref={(c) => { this.menuCalendar = c; }} />
      // </div>
