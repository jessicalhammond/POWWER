import React from 'react';
import App from '../components/App';

// Child routes
import home from './home';
import login from './login';
import signup from './signUp';
import ErrorPage from './error/ErrorPage';

// dashboard pages
import typography from './dashboard/Typography';
import grid from './dashboard/Grid';
import table from './dashboard/Table';
import formComponents from './dashboard/Forms/Components';
import formElements from './dashboard/Forms/Elements';
import buttons from './dashboard/UIElement/Buttons';
import dropdowns from './dashboard/UIElement/DropDowns';
import icons from './dashboard/UIElement/Icons';
import panels from './dashboard/UIElement/Panels';
import alerts from './dashboard/UIElement/Alerts';
import progressbars from './dashboard/UIElement/Progressbars';
import paginations from './dashboard/UIElement/Pagination';
import otherElements from './dashboard/UIElement/OtherElement';
import chartjs from './dashboard/Charts/Chartjs';
import morris from './dashboard/Charts/MorrisChart';
import inbox from './dashboard/Mail/Inbox';
import compose from './dashboard/Mail/Compose';
import blank from './dashboard/Blank';
import profile from './dashboard/Profile';
import invoice from './dashboard/Invoice';
import calendar from './dashboard/Calendar';


export default [{

  path: '/',

  // keep in mind, routes are evaluated in order
  children: [
    home,
    typography,
    grid,
    table,
    formComponents,
    formElements,
    buttons,
    dropdowns,
    icons,
    panels,
    alerts,
    progressbars,
    paginations,
    otherElements,
    chartjs,
    morris,
    inbox,
    compose,
    blank,
    profile,
    invoice,
    calendar,

  ],

  async action({ next, render, context }) {
    const component = await next();
    if (component === undefined || component === true) return component;
    return render(
      <App context={context} header={true}>{component}</App> // eslint-disable-line
    );
  },
},

{
  path: '/',

  children: [
    signup,
    login,
  ],

  async action({ next, render, context }) {
    const component = await next();
    if (component === undefined) return component;
    return render(
      <App context={context} header={false}>{component}</App>
    );
  },
},

{
  path: '/error',

  action({ render, context, error }) {
    return render(
      <App context={context} error={error}>
        <ErrorPage error={error} />
      </App>,
      error.status || 500
    );
  },
},


];
