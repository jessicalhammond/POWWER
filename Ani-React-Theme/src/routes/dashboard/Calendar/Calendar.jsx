import React, { Component, PropTypes } from 'react';
import FullCalendar from 'rc-calendar/lib/FullCalendar';
import Select from 'rc-select';
import moment from 'moment';
// import zhCN from 'rc-calendar/lib/locale/zh_CN';
// import enUS from 'rc-calendar/lib/locale/en_US';
// import 'moment/locale/zh-cn';
// import 'moment/locale/en-gb';


// const cn = location.search.indexOf('cn') !== -1;

const title = 'Calendar';
const now = moment();

const defaultCalendarValue = now.clone();
defaultCalendarValue.add(-1, 'month');

class SidebarCalendar extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  constructor(props) {
    super(props);
    this.state = {
      type: 'month',
    };
    this.onTypeChange = this.onTypeChange.bind(this);
  }

  componentWillMount() {
    this.context.setTitle(title);
  }
  componentDidMount() {

  }
  onTypeChange(type) {
    this.setState({
      type,
    });
  }
  render() {
    return (
      <div className="animate" style={{ zIndex: 1000, position: 'relative' }}>
        <FullCalendar
          style={{ margin: 10 }}
          Select={Select}
          fullscreen
          defaultValue={now}
        />
      </div>
    );
  }
}

export default SidebarCalendar;
