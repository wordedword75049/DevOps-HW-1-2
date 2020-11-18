import React, { PureComponent } from 'react';
import _isEqual from 'lodash/isEqual';
import { ISelectTarget } from '@bostongene/frontend-components/Select';
import DrugsListFiltersPage from './DrugsListFiltersPage';

export interface IFilterData {
  maxDrugPhase: string;
}

interface IDrugsListFiltersProps {
  onFilterDataChange: (data: IFilterData) => void;
}

interface IDrugsListFiltersState {
  filterData: IFilterData;
}

class DrugsListFilters extends PureComponent<IDrugsListFiltersProps, IDrugsListFiltersState> {
  state = {
    filterData: {
      maxDrugPhase: ''
    }
  };

  componentDidUpdate(prevProps: IDrugsListFiltersProps, prevState: IDrugsListFiltersState) {
    const { onFilterDataChange } = this.props;
    const { filterData } = this.state;

    if (!_isEqual(prevState.filterData, filterData)) {
      onFilterDataChange(filterData);
    }
  }

  onPhaseChange = ({ name, value }: ISelectTarget) => {
    this.setState({ filterData: { ...this.state.filterData, [name]: value } });
  };

  render() {
    const { filterData } = this.state;

    return <DrugsListFiltersPage filterData={filterData} onPhaseChange={this.onPhaseChange} />;
  }
}

export default DrugsListFilters;
